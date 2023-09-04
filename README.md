# Google File System (GFS) Implemented in Go

This is a simplified version of the Google File System (GFS) implemented in Go.
The original paper can be found [here](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf).
The framework given in [this lab](https://bitbucket.org/abcdabcd987/ppca-gfs/) was used as a starting point.

The [implementation notes](docs/notes.md) contain some explanations of GFS and some implementation details.

## Testing

Currently, the project passes the graybox test from the original lab. To run the test, run `go test -v` in the `/gfs` directory.
