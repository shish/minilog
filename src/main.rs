use anyhow::Result;
use std::fs::File;
use std::io;
use zstd::stream::read::Decoder;
use zstd::stream::write::Encoder;

#[macro_use]
extern crate clap;

pub mod lib;

fn main() -> Result<()> {
    let matches = clap_app!(myapp =>
        (version: "1.0")
        (author: "Shish <s@shish.io>")
        (about: "Deal with minilog files")
        // (@arg debug: -d ... "Sets the level of debugging information")
        (@subcommand log2mlg =>
            (about: "Turn apache logs into binary (timestamp, ip) tuples")
            (@arg INPUT: "Sets the input file to use")
            (@arg OUTPUT: "Sets the output file to use")
            // (@arg verbose: -v --verbose "Print test information verbosely")
        )
        (@subcommand mlg2dau =>
            (about: "Show DAU from MLG log file")
            (@arg INPUT: "Sets the input file to use")
            (@arg OUTPUT: "Sets the output file to use")
        )
        (@subcommand mlg2mau =>
            (about: "Show MAU from MLG log file")
            (@arg INPUT: "Sets the input file to use")
            (@arg OUTPUT: "Sets the output file to use")
        )
        (@subcommand mlg2uniq =>
            (about: "Show Unique IPs from MLG log file")
            (@arg INPUT: "Sets the input file to use")
            (@arg OUTPUT: "Sets the output file to use")
        )
    )
    .get_matches();

    if let Some(matches) = matches.subcommand_matches("log2mlg") {
        let reader = get_input(matches)?;
        let writer = get_output(matches)?;
        minilog::log2mlg(reader, writer)?;
    }

    if let Some(matches) = matches.subcommand_matches("mlg2dau") {
        let reader = get_input(matches)?;
        let writer = get_output(matches)?;
        minilog::mlg2dau(reader, writer)?;
    }

    if let Some(matches) = matches.subcommand_matches("mlg2mau") {
        let reader = get_input(matches)?;
        let writer = get_output(matches)?;
        minilog::mlg2mau(reader, writer)?;
    }

    if let Some(matches) = matches.subcommand_matches("mlg2uniq") {
        let reader = get_input(matches)?;
        let writer = get_output(matches)?;
        minilog::mlg2uniq(reader, writer)?;
    }

    Ok(())
}

fn get_input(matches: &clap::ArgMatches) -> Result<io::BufReader<Box<dyn io::Read>>> {
    let filename = matches.value_of("INPUT");
    let reader: Box<dyn io::Read> = match filename {
        Some("-") | None => Box::new(io::stdin()),
        Some(filename) => {
            let mut reader: Box<dyn io::Read> =
                Box::new(File::open(filename).expect(&(format!("Error opening {}", filename))));
            if filename.ends_with(".zst") {
                reader = Box::new(Decoder::new(reader)?)
            }
            reader
        }
    };
    Ok(io::BufReader::new(reader))
}

fn get_output(matches: &clap::ArgMatches) -> Result<io::BufWriter<Box<dyn io::Write>>> {
    let filename = matches.value_of("OUTPUT");
    let writer: Box<dyn io::Write> = match filename {
        Some("-") | None => Box::new(io::stdout()),
        Some(filename) => {
            let mut writer: Box<dyn io::Write> =
                Box::new(File::create(filename).expect(&(format!("Error opening {}", filename))));
            if filename.ends_with(".zst") {
                writer = Box::new(Encoder::new(writer, 0)?)
            }
            writer
        }
    };
    Ok(io::BufWriter::new(writer))
}
