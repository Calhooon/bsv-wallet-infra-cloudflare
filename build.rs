//! The build object GET /healthz serves: the revision, whether the tree was dirty, the
//! build time and the locked versions of bsv-rs and the middleware, from the shared
//! capture (bsv-worker-build-info). No runtime I/O.
fn main() {
    bsv_worker_build_info::emit();
}
