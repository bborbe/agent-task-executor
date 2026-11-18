// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import "time"

// SuccessGracePeriod is how long a claim stays held after its Job succeeded.
//
// A succeeded Job means the agent published its result, but the vault has not
// necessarily absorbed it yet — the controller writes phase and status back on
// its own cycle. In that gap a peer cluster sees a task that still reads as
// eligible, and nothing tells it otherwise: the spawn path's success record is
// per-pod memory, so a cluster that starts cold has never heard of the result.
//
// Holding the claim across the gap is what stops the peer, because the claim ref
// is the one artefact both clusters share. A peer's Claim inside the window gets
// a 409 and does not spawn; once the window has passed the vault carries the
// result and the normal phase gate decides.
//
// It matches the spawn path's respawn grace window — defaultRespawnGracePeriod
// aliases this constant — so the two bound one gap from opposite ends: one
// refuses to spawn, the other refuses to hand the claim over.
//
// 300 s was measured to be shorter than the gap it guards. Across the
// 2026-11-15 window, nine of the twelve cross-cluster duplicates re-spawned at
// +343 to +355 s after the earlier run ended — 43 to 55 s *after* a 300 s grace
// had already run out, with the vault still not carrying the result. A grace
// that expires inside the gap it exists to cover turns every post-success
// duplicate into an expected outcome instead of a bug.
//
// 900 s is the measured band plus roughly a 2.5x margin: wide enough to survive
// a slower vault sync, narrow enough that it does not delay a retry. It cannot
// delay a *failed* task at all — a failure releases its claim through the
// release paths, never through this grace — so the only work this holds back is
// a re-run of something that already succeeded.
const SuccessGracePeriod = 900 * time.Second
