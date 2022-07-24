use criterion::{criterion_group, criterion_main, Criterion};
use minilog::*;
use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;

fn all(c: &mut Criterion) {
    let input = "213.180.203.32 - - [31/Aug/2020:00:00:39 +0000] \"GET ...\n".repeat(1_000_000);
    let data = input.as_bytes();
    let mut group = c.benchmark_group("from_log");
    group.sample_size(10);
    group.throughput(criterion::Throughput::Bytes(data.len() as u64));
    group.bench_function("log2mlg", |b| b.iter(|| log2mlg(data, std::io::sink())));
    group.finish();


    let mut small_rng = SmallRng::seed_from_u64(42);
    let mut buf = [0u8; 8_000_000];
    let mut ts: u32 = 0;
    for n in 0..1_000_000 {
        let tsb = ts.to_be_bytes();
        buf[n*8+0] = tsb[0];
        buf[n*8+1] = tsb[1];
        buf[n*8+2] = tsb[2];
        buf[n*8+3] = tsb[3];
        buf[n*8+4] = 1;
        buf[n*8+5] = 1;
        buf[n*8+6] = small_rng.gen();
        buf[n*8+7] = small_rng.gen();
        ts += small_rng.gen::<u32>() % 4;
    }
//    small_rng.fill(&mut buf[..]);
    let data = &buf[..];

    let mut group = c.benchmark_group("from_mlg");
    group.throughput(criterion::Throughput::Bytes(data.len() as u64));
    group.bench_function("mlg2mau", |b| b.iter(|| mlg2mau(data, std::io::sink())));
    group.bench_function("mlg2dau", |b| b.iter(|| mlg2dau(data, std::io::sink())));
    group.bench_function("mlg2uniq", |b| b.iter(|| mlg2uniq(data, std::io::sink())));
    group.finish();
}

criterion_group!(benches, all);
criterion_main!(benches);
