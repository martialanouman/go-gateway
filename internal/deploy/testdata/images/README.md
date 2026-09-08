Deliberately broken release configuration and manifests. Every rule in images_test.go violates here at
least once, so TestTheImageGuardCatchesWhatItClaimsTo can prove the guard still bites. Never fix
anything in this directory: a green fixture makes the guard a test nobody has seen fail.

Held apart from testdata/broken/ on purpose. That tree is walked by deploy.Load for the eleven manifest
rules; dropping a goreleaser.yaml into it would work by accident, because decodeFile skips documents
with no Kind — and leaning on an accident is exactly what this repository writes its comments to avoid.

  k8s/svc.yaml           image-a  no build at all                    -> image-has-build
                         image-b  built, but tags: ["latest"]        -> image-has-docker-entry
                         image-c  pinned to :v1.2.3, not :v0.0.0     -> image-tag-placeholder
  k8s/jobs/migrate.yaml  migrate  entry without extra_files          -> migrations-in-migrate-image
  goreleaser.yaml        orphan   published, deployed nowhere        -> docker-entry-is-deployed
  Dockerfile             FROM alpine, no USER                       -> image-nonroot
  Dockerfile.migrate     distroless but USER 0, no COPY migrations  -> image-nonroot,
                                                                       migrations-in-migrate-image
