use criterion::{criterion_group, criterion_main, Criterion};
use minilog::*;

fn all(c: &mut Criterion) {
    let input = "213.180.203.32 - - [31/Aug/2020:00:00:39 +0000] \"GET ...\n".repeat(1000000);
    let data = input.as_bytes();
    c.bench_function("log2mlg", |b| b.iter(|| log2mlg(data, std::io::sink())));
}

criterion_group!(benches, all);
criterion_main!(benches);
