package datatrails

const (
	V1MMRPrefix = "v1/mmrs"

	V1MMRBlobNameFmt                 = "%016d.log"
	V1MMRSignedTreeHeadBlobNameFmt   = "%016d.sth"

	// Note: this is due to datatrails tenant schema
	V1MMRTenantPrefix = "v1/mmrs/tenant"
	LogInstanceN	 = 0 // the log instance number, used to identify the log in the massif path
)
