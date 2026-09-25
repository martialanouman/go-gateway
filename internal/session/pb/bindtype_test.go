package pb

import "testing"

func TestBindTypeNameStaysInsideTheContractEnum(t *testing.T) {
	for bt, want := range map[BindType]string{
		BindType_BIND_TYPE_TX: "tx", BindType_BIND_TYPE_RX: "rx", BindType_BIND_TYPE_TRX: "trx",
		BindType_BIND_TYPE_UNSPECIFIED: "", BindType(5): "",
	} {
		if got := bt.Name(); got != want {
			t.Errorf("%v.Name() = %q, want %q", bt, got, want)
		}
	}
}
