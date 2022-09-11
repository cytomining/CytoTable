// project cuefile for Dagger CI and other development tooling related to this project.
package main

import (
	"dagger.io/dagger"
	"universe.dagger.io/docker"
	"universe.dagger.io/bash"
)

// python build for linting, testing, building, etc.
#PythonBuild: {
	// client filesystem
	filesystem: dagger.#FS

	// python version to use for build
	python_ver: string | *"3.9"

	// poetry version to use for build
	poetry_ver: string | *"1.2.0"

	// container image
	output: _python_build.output

	// referential build for base python image
	_python_pre_build: docker.#Build & {
		steps: [
			docker.#Pull & {
				source: "python:" + python_ver
			},
			docker.#Run & {
				command: {
					name: "mkdir"
					args: ["/workdir"]
				}
			},
			docker.#Copy & {
				contents: filesystem
				source:   "./pyproject.toml"
				dest:     "/workdir/pyproject.toml"
			},
			docker.#Copy & {
				contents: filesystem
				source:   "./poetry.lock"
				dest:     "/workdir/poetry.lock"
			},
			docker.#Run & {
				workdir: "/workdir"
				command: {
					name: "pip"
					args: ["install", "--no-cache-dir", "poetry==" + poetry_ver]
				}
			},
			docker.#Set & {
				config: {
					env: ["POETRY_VIRTUALENVS_CREATE"]: "false"
				}
			},
			docker.#Run & {
				workdir: "/workdir"
				command: {
					name: "poetry"
					args: ["install", "--no-interaction", "--no-ansi"]
				}
			},
		]
	}
	// python build with likely changes
	_python_build: docker.#Build & {
		steps: [
			docker.#Copy & {
				input:    _python_pre_build.output
				contents: filesystem
				source:   "./"
				dest:     "/workdir"
			},
		]
	}
}

// Convenience cuelang build for formatting, etc.
#CueBuild: {
	// client filesystem
	filesystem: dagger.#FS

	// output from the build
	output: _cue_build.output

	// cuelang pre-build
	_cue_pre_build: docker.#Build & {
		steps: [
			docker.#Pull & {
				source: "golang:latest"
			},
			docker.#Run & {
				command: {
					name: "mkdir"
					args: ["/workdir"]
				}
			},
			docker.#Run & {
				command: {
					name: "go"
					args: ["install", "cuelang.org/go/cmd/cue@latest"]
				}
			},
		]
	}
	// cue build for actions in this plan
	_cue_build: docker.#Build & {
		steps: [
			docker.#Copy & {
				input:    _cue_pre_build.output
				contents: filesystem
				source:   "./project.cue"
				dest:     "/workdir/project.cue"
			},
		]
	}

}

dagger.#Plan & {

	client: {
		filesystem: {
			"./": read: contents:             dagger.#FS
			"./project.cue": write: contents: actions.clean.cue.export.files."/workdir/project.cue"
			"./tests/data": write: contents:  actions.test.cellprofiler_out.export.directories."/usr/local/src/output"
		}
	}
	python_version: string | *"3.9"
	poetry_version: string | *"1.2.0"

	actions: {

		python_build: #PythonBuild & {
			filesystem: client.filesystem."./".read.contents
			python_ver: python_version
			poetry_ver: poetry_version
		}

		cue_build: #CueBuild & {
			filesystem: client.filesystem."./".read.contents
		}

		cellprofiler_build: docker.#Pull & {
			source: "cellprofiler/cellprofiler"
		}

		// applied code and/or file formatting
		clean: {
			// code formatting for cuelang
			cue: docker.#Run & {
				input:   cue_build.output
				workdir: "/workdir"
				command: {
					name: "cue"
					args: ["fmt", "/workdir/project.cue"]
				}
				export: {
					files: "/workdir/project.cue": _
				}
			}
		}

		// linting to check for formatting and best practices
		test: {
			cellprofiler_out: bash.#Run & {
				input: cellprofiler_build.output
				script: contents: """
					# get example from https://cellprofiler.org/examples
					wget https://cellprofiler-examples.s3.amazonaws.com/ExampleHuman.zip -O ExampleHuman.zip

					# unzip
					jar xvf ExampleHuman.zip

					# make output dir
					mkdir output

					# run cellprofiler against example pipeline
					# commands reference: https://github.com/CellProfiler/CellProfiler/wiki/Getting-started-using-CellProfiler-from-the-command-line
					cellprofiler -c -r -p ExampleHuman/ExampleHuman.cppipe -o output -i ExampleHuman/images
					"""
				export: {
					directories: {"/usr/local/src/output": _}
				}
			}

		}
	}
}
