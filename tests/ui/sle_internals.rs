// The service instances a registry holds are only looked up by the
// association actor.
use groundlink::SleServiceRegistry;
use groundlink::actor::sle::association::ServiceInstanceRef;

fn main() {
    let registry = SleServiceRegistry::new();
    let _ = registry.get(&"sagr=1.spack=2.rsl-fg=3.raf=onlt1".parse().unwrap());
}
