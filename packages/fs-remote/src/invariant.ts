/**
 * Package invariant companion. The provider's contract — that every method maps
 * to one sidecar round trip and typed errors round-trip as FsError codes — is
 * asserted by the provider tests against a live sidecar, not by a static probe.
 * @module dsh-fs-remote/invariant
 */

/** No runtime invariant: method/error mapping is covered by the provider tests. */
export function install(): void {}
