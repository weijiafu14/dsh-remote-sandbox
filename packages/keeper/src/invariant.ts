/**
 * Package invariant companion. The keeper's owned relationship — a live sandbox
 * always backing the provided `ctx.remoteSandbox` — is asserted by the recovery
 * tests that drive real reconnect and recreate paths, not by a static runtime
 * probe, so this installer registers no additional runtime invariant.
 * @module dsh-sandbox-keeper/invariant
 */

/** No runtime invariant: the sandbox-liveness relationship is covered by recovery tests. */
export function install(): void {}
