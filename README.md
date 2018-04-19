# escat
Another command-line interface for the [Elasticsearch _cat API][1].

Inspired by the deprecated [es2unix][2]. The alternative [copycat][3] seems like a better solution than this, but it interacts poorly with my environment variables for some reason. Also, for some reason I wanted to explore the intricacies of python `argparse`.

The name seemed like a good idea at the time, at least until I first used the command. Oh well, too late now...

Try other solutions first. They probably work better.

## Notes
* python3
  * Eh, I like type hinting for documentation clarity...
* Requires [elasticsearch-py][4]
  * Should not require it, but I used it because I had it
* Changes to _cat API will break this
  * At some point I might come back and change this...

[1]: https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
[2]: https://github.com/elastic/es2unix
[3]: https://github.com/drewr/copycat
[4]: https://elasticsearch-py.readthedocs.io/en/latest/
