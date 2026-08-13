/**
 * Package invariant companion. The provider's owned relationships — bounded
 * collected output and tree-scoped exit facts — are asserted by the provider
 * tests driving a live sidecar, not by a static probe.
 * @module dsh-subprocess-remote/invariant
 */

/** No runtime invariant: output bounds and exit facts are covered by the provider tests. */
export function install(): void {}
