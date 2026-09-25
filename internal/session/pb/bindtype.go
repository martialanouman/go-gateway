package pb

// Name is the Admin contract's name for a bind type — tx, rx or trx — and "" for any other value, so an
// unset or future type never reaches the API as a value outside the contract's enum.
func (t BindType) Name() string {
	switch t {
	case BindType_BIND_TYPE_TX:
		return "tx"
	case BindType_BIND_TYPE_RX:
		return "rx"
	case BindType_BIND_TYPE_TRX:
		return "trx"
	default:
		return ""
	}
}
