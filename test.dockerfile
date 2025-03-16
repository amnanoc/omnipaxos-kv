FROM rust:latest

WORKDIR /app

# Copy the source code
COPY . .

# Install dependencies
RUN cargo build --release

# Run tests
CMD ["cargo", "test", "--", "--test-threads=1"]