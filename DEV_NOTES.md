# Development Notes

## FUSE3 toolchain availability

The default execution environment for this repository does not ship with the
`libfuse3` development headers or libraries. Package installation via `apt`
also is disabled, so targets that require the system headers (such as the
`appendfsd` FUSE daemon) cannot be compiled inside the sandbox.

The existing `Makefile` detects this situation automatically: the `appendfsd`
target is skipped whenever `pkg-config --cflags fuse3` fails, while the rest of
the project (including the metadata prototype) continues to build normally.

## Testing strategy inside the sandbox

Because the C implementation depends on libfuse3, substituting Python FUSE
bindings (for example `fusepy` or `llfuse`) will not exercise the same code
paths and therefore cannot be used as a drop-in replacement for validating the
C FUSE server. The recommended approach is:

1. Run the metadata prototype (`./prototype <store-dir>`) to validate log
   replay, buffering, and persistence behaviour.
2. Develop and run the full FUSE daemon (`appendfsd`) in an environment where
   libfuse3 is available (such as a local development machine or container
   image with `libfuse3-dev` preinstalled).

When such an environment is not available, focus on the prototype-level tests
and static analysis. Full end-to-end FUSE validation requires access to the
actual libfuse3 headers and kernel support, which is outside the capabilities
of the restricted sandbox.
