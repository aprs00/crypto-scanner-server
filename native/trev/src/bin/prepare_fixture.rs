//! Convert the original 33-byte normalized benchmark tape to projector RowBinary.
//! Prices/quantities stay in native units (use divisors 1/1); source rank is retained.
use std::{
    env,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
};

fn main() -> io::Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: prepare_fixture NORMALIZED_33_BYTE_INPUT OUTPUT_ROWBINARY");
        std::process::exit(2);
    }
    let mut input = BufReader::new(File::open(&args[1])?);
    let mut output = BufWriter::new(File::create(&args[2])?);
    let mut record = [0u8; 33];
    let mut rank = 0u64;
    loop {
        let mut used = 0;
        while used < record.len() {
            let n = input.read(&mut record[used..])?;
            if n == 0 {
                break;
            }
            used += n;
        }
        if used == 0 {
            break;
        }
        if used != 33 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "partial fixture record",
            ));
        }
        let id = rank.to_string();
        output.write_all(&record[..4])?;
        output.write_all(&rank.to_le_bytes())?;
        output.write_all(&0u32.to_le_bytes())?;
        output.write_all(&record[4..12])?;
        output.write_all(&[id.len() as u8])?;
        output.write_all(id.as_bytes())?;
        output.write_all(&record[12..29])?;
        let spec = u32::from_le_bytes(record[29..33].try_into().expect("fixed record"));
        output.write_all(&u64::from(spec).to_le_bytes())?;
        rank += 1;
    }
    output.flush()?;
    eprintln!("{{\"rows\":{rank}}}");
    Ok(())
}
