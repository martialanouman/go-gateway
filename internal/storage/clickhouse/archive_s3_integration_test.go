package clickhouse_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
	"github.com/martialanouman/go-gateway/internal/testutil/ciguard"
)

const (
	rustfsImage = "rustfs/rustfs:1.0.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff"
	mcImage     = "cgr.dev/chainguard/minio-client@sha256:b2bd7824d23d3e3b15bedd7e87fbc3be29d2e213307b4f901e4a1d92356dc20f"

	// The archive identity, as testdata/s3archive/named_collections.xml hands it to ClickHouse.
	archiveAccessKey = "ARCHIVEKEY407"
	archiveSecretKey = "archive-secret-407"
)

// startS3Archive runs RustFS and a ClickHouse whose server config declares the named collections, and
// provisions what step-410 asks of the operator: a bucket, and an identity that may write and read under it
// but not delete.
func startS3Archive(t *testing.T) *clickhouse.Conn {
	t.Helper()
	if testing.Short() {
		ciguard.Skip(t, "s3 archive: skipped under -short (needs Docker)")
	}
	ciguard.RequireDocker(t)
	ctx := context.Background()

	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("network: %v", err)
	}
	testcontainers.CleanupNetwork(t, nw)

	s3, err := testcontainers.Run(ctx, rustfsImage,
		network.WithNetwork([]string{"s3"}, nw),
		testcontainers.WithEnv(map[string]string{"RUSTFS_ACCESS_KEY": "rootadmin", "RUSTFS_SECRET_KEY": "rootsecret"}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9000/tcp")))
	testcontainers.CleanupContainer(t, s3)
	if err != nil {
		t.Fatalf("rustfs: %v", err)
	}

	mc(t, nw, "mb", "root/cdr-archive")
	mc(t, nw, "mb", "root/cdr-forbidden")
	mc(t, nw, "admin", "user", "add", "root", archiveAccessKey, archiveSecretKey)
	mc(t, nw, "admin", "policy", "create", "root", "cdr-archive", "/policy.json")
	mc(t, nw, "admin", "policy", "attach", "root", "cdr-archive", "--user", archiveAccessKey)

	cfg := chtest.Start(t,
		network.WithNetwork(nil, nw),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{HostFilePath: "testdata/s3archive/named_collections.xml",
				ContainerFilePath: "/etc/clickhouse-server/config.d/named_collections.xml", FileMode: 0o644},
			testcontainers.ContainerFile{HostFilePath: "testdata/s3archive/grants.xml",
				ContainerFilePath: "/etc/clickhouse-server/users.d/zz-grants.xml", FileMode: 0o644}))
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// mc runs one MinIO-client command against RustFS as its root user.
func mc(t *testing.T, nw *testcontainers.DockerNetwork, args ...string) {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.Run(ctx, mcImage,
		network.WithNetwork(nil, nw),
		testcontainers.WithEnv(map[string]string{"MC_HOST_root": "http://rootadmin:rootsecret@s3:9000"}),
		testcontainers.WithFiles(testcontainers.ContainerFile{HostFilePath: "testdata/s3archive/archive_policy.json",
			ContainerFilePath: "/policy.json", FileMode: 0o644}),
		testcontainers.WithCmd(args...),
		testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(30*time.Second)))
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("mc %v: %v", args, err)
	}
	state, err := c.State(ctx)
	if err != nil {
		t.Fatalf("mc %v: state: %v", args, err)
	}
	if state.ExitCode != 0 {
		logs, _ := c.Logs(ctx)
		out, _ := io.ReadAll(logs)
		t.Fatalf("mc %v exited %d: %s", args, state.ExitCode, out)
	}
}

func TestPartitionArchiverOnObjectStorage(t *testing.T) {
	conn := startS3Archive(t)

	t.Run("an expired partition is archived, read back, catalogued, then dropped", func(t *testing.T) {
		ctx := context.Background()
		day := seedDay(t, conn, 70, 5)
		catalog := newArchiveCatalog()
		retainer := clickhouse.NewRetainer(conn, retentionKeepsToday, clickhouse.WithArchiver(
			clickhouse.NewPartitionArchiver(conn, "cdr", clickhouse.S3Destination("cdr_archive"), catalog)))

		if _, err := retainer.Purge(ctx); err != nil {
			t.Fatalf("Purge: %v", err)
		}
		if n := countDay(t, conn, day); n != 0 {
			t.Errorf("partition holds %d rows, want 0 once its archive is catalogued", n)
		}
		var archived uint64
		dest := clickhouse.S3Destination("cdr_archive")(catalog.object(t, day))
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+dest).Scan(&archived); err != nil {
			t.Fatalf("read the catalogued object back: %v", err)
		}
		if archived != 5 {
			t.Errorf("catalogued object holds %d rows, want the partition's 5", archived)
		}
	})

	t.Run("a write the storage refuses keeps the partition", func(t *testing.T) {
		ctx := context.Background()
		day := seedDay(t, conn, 80, 2)
		catalog := newArchiveCatalog()
		retainer := clickhouse.NewRetainer(conn, retentionKeepsToday, clickhouse.WithArchiver(
			clickhouse.NewPartitionArchiver(conn, "cdr", clickhouse.S3Destination("cdr_archive_denied"), catalog)))

		if _, err := retainer.Purge(ctx); err != nil {
			t.Fatalf("Purge: %v", err)
		}
		if n := countDay(t, conn, day); n != 2 {
			t.Errorf("partition holds %d rows, want its 2 kept", n)
		}
		if len(catalog.entries) != 0 {
			t.Errorf("catalogue holds %v, want nothing recorded for a refused write", catalog.entries)
		}
		err := clickhouse.NewPartitionArchiver(conn, "cdr", clickhouse.S3Destination("cdr_archive_denied"), catalog).
			Archive(ctx, clickhouse.Partition{Day: day, Rows: 2})
		// 499 is S3_ERROR, the storage answering 403 to the existence check; a missing collection grant is 497.
		if err == nil || !strings.Contains(err.Error(), "code: 499") {
			t.Errorf("Archive() = %v, want the storage's own refusal (code 499)", err)
		}
	})

	t.Run("no statement carries the S3 identity", func(t *testing.T) {
		ctx := context.Background()
		if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatalf("flush logs: %v", err)
		}
		var archiving, leaking uint64
		if err := conn.QueryRow(ctx, `SELECT countIf(query LIKE 'INSERT INTO FUNCTION s3(%'),
				countIf(position(query, ?) > 0 OR position(query, ?) > 0)
			FROM system.query_log WHERE query NOT LIKE '%system.query_log%'`,
			archiveAccessKey, archiveSecretKey).Scan(&archiving, &leaking); err != nil {
			t.Fatalf("read query_log: %v", err)
		}
		if archiving == 0 {
			t.Fatal("query_log holds no archiving statement: the check below would prove nothing")
		}
		if leaking != 0 {
			t.Errorf("%d logged statements carry the S3 access key or secret", leaking)
		}
	})
}
