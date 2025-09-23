<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# How to Contribute

All contributors are expected to follow the [Code of
Conduct](CODE_OF_CONDUCT.md).

## Reporting Issues

Please file issues and feature requests on the GitHub issue tracker:
https://github.com/adbc-drivers/trino/issues

Potential security vulnerabilities should be reported to
[security@adbc-drivers.org](mailto:security@adbc-drivers.org) instead.  See
[SECURITY.md](./SECURITY.md).

## Build and Test

For basic development, the driver can be built and tested like any Go project.

```shell
$ go build ./...
$ go test -tags assert -v ./...
```

This will not produce a shared library, however; that requires invoking the
full build script.  You will need [pixi](https://pixi.sh/) installed.  From
the repository root:

```shell
$ pixi run make
```

To run the validation suite, you will first need to build the shared library.
You will also need to set up a Trino instance (see [the validation
README](./validation/README.md)).  Finally, from the `validation/`
subdirectory:

```shell
$ pixi run test
```

This will produce a test report, which can be rendered into a documentation
page (using MyST Markdown):

```shell
$ pixi run gendocs --output generated/
```

Then look at `./generated/trino.md`.

## Opening a Pull Request

Before opening a pull request:

- Review your changes and make sure no stray files, etc. are included.
- Ensure the Apache license header is at the top of all files.
- Check if there is an existing issue.  If not, please file one, unless the
  change is trivial.
- Assign the issue to yourself by commenting just the word `take`.
- Run the static checks by installing [pre-commit](https://pre-commit.com/),
  then running `pre-commit run --all-files` from inside the repository.  Make
  sure all your changes are staged/committed (unstaged changes will be
  ignored).

When writing the pull request description:

- Ensure the title follows [Conventional
  Commits](https://www.conventionalcommits.org/en/v1.0.0/) format.  The
  component is not necessary (in general: it should be a directory path
  relative to the repo root).  Example titles:

  - `feat: support new data type`
  - `chore: update action versions`
  - `fix!: return ns instead of us`

  Ensure that breaking changes are appropriately flagged with a `!` as seen
  in the last example above.
- Make sure the bottom of the description has `Closes #NNN`, `Fixes #NNN`, or
  similar, so that the issue will be linked to your pull request.
