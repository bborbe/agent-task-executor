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
// It matches the spawn path's respawn grace window: the two bound one gap from
// opposite ends — one refuses to spawn, the other refuses to hand the claim over.
const SuccessGracePeriod = 300 * time.Second
