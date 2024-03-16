# Rubric

Yet another [1BRC](https://github.com/gunnarmorling/1brc) implementation in Rust.

Actually this is a "learning" take to a port of [this elegant solution in Go](https://gist.github.com/corlinp/176a97c58099bca36bcd5679e68f9708) by [Corlin Palmer](https://github.com/corlinp). Trying to implement the solution in other languages (I have [another one in Elixir](https://github.com/arhyth/brex)) in hope of understanding what makes it performant.

Shoutout to a colleague at the Philippine Rust community, @FrancisMurillo, from whom I've stolen the implementation of an object pool for `Vec<u8>` chunks to mitigate the performance cost of allocation. This underlies the main (and hidden) optimization of the original Go implementation which could not be overcome by my Elixir implementation even with the use of a Rust NIF.