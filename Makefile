# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ifeq ($(shell go env GOOS), linux)
	RM=rm -f
	PREFIX=lib
	SUFFIX=so
else ifeq ($(shell go env GOOS), darwin)
	RM=rm -f
	PREFIX=lib
	SUFFIX=dylib
else ifeq ($(shell go env GOOS), windows)
	RM=del
	PREFIX=
	SUFFIX=dll
else
	$(error Unsupported OS)
endif

DRIVERS := \
	libadbc_driver_trino.$(SUFFIX)

.PHONY: all clean
all: $(DRIVERS)

clean:
	$(RM) $(DRIVERS)

libadbc_driver_trino.$(SUFFIX): $(wildcard *.go pkg/*.go pkg/*.c pkg/%.h) go.mod go.sum
	go build -C ./pkg -o ../$@ -buildmode=c-shared -tags driverlib -ldflags "-s -w" .
	-$(RM) ./$(basename $@).h
	chmod 755 $@
