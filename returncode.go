package unitdb

import "fmt"

// Return codes of a CONNECT acknowledgement, as the server sends them (see
// the uTP spec, docs/utp.md in the unitdb repository). Connect fails with a
// *ConnectError holding the code for any code but ConnAccepted.
//
// The server's utp package names code 0x04 ErrRefusedServerUnavailable, but
// the server sends it when the client is not authorized: a refused topic
// key, or the insecure flag on a server that does not allow it.
const (
	// ConnAccepted: the connection is accepted.
	ConnAccepted uint8 = 0x00
	// ConnRefusedBadProtocolVersion: the protocol version is not supported.
	ConnRefusedBadProtocolVersion uint8 = 0x01
	// ConnRefusedIDRejected: the client id is not valid, or has expired or
	// been revoked, or is a v1 id, which servers refuse since v0.7.0. An
	// expired or revoked id needs a new one, which the contract's primary
	// client requests (see Client.RequestClientID).
	ConnRefusedIDRejected uint8 = 0x02
	// ConnRefusedBadID: the client id is not allowed access.
	ConnRefusedBadID uint8 = 0x03
	// ConnRefusedNotAuthorized: the client is not authorized, for example
	// the insecure flag on a server without allow_insecure.
	ConnRefusedNotAuthorized uint8 = 0x04
	// ConnRefusedServerError: the server failed.
	ConnRefusedServerError uint8 = 0x05
	// ConnRefusedAuthFailed: the authentication failed.
	ConnRefusedAuthFailed uint8 = 0x06
	// ConnRefusedForbidden: the connection is forbidden.
	ConnRefusedForbidden uint8 = 0x07
	// ConnRefusedSessionInUse: the session is in use by another connection.
	ConnRefusedSessionInUse uint8 = 0x08
	// ConnRefusedUnknownEpoch: the epoch is unknown.
	ConnRefusedUnknownEpoch uint8 = 0x09

	// ConnNotAcknowledged is not a code the server sends: Connect returns
	// it, with an error, when it could not read the server's
	// acknowledgement at all.
	ConnNotAcknowledged uint8 = 0xFF
)

// returnCodeText names the return codes.
var returnCodeText = map[uint8]string{
	ConnAccepted:                  "connection accepted",
	ConnRefusedBadProtocolVersion: "unacceptable proto version",
	ConnRefusedIDRejected:         "identifier rejected",
	ConnRefusedBadID:              "unacceptable identifier, access not allowed",
	ConnRefusedNotAuthorized:      "not authorized",
	ConnRefusedServerError:        "server error",
	ConnRefusedAuthFailed:         "authentication failed",
	ConnRefusedForbidden:          "forbidden",
	ConnRefusedSessionInUse:       "session in use by another connection",
	ConnRefusedUnknownEpoch:       "unknown epoch",
	ConnNotAcknowledged:           "not acknowledged",
}

// ReturnCodeText returns a description of a CONNECT return code.
func ReturnCodeText(code uint8) string {
	if text, ok := returnCodeText[code]; ok {
		return text
	}
	return "unknown return code"
}

// ConnectError is the error of a connection the server refused. Use
// errors.As to read its return code, for example to tell an expired or
// revoked client id (ConnRefusedIDRejected) from other failures.
type ConnectError struct {
	// Server is the host of the server that refused the connection.
	Server string
	// ReturnCode is the return code the server answered with.
	ReturnCode uint8
}

func (e *ConnectError) Error() string {
	return fmt.Sprintf("connection to %s refused, return code %d (%s)", e.Server, e.ReturnCode, ReturnCodeText(e.ReturnCode))
}
