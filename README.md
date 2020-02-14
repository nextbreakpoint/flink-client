# flink-client

This library provides a Java client for managing Apache Flink via the [Monitoring REST API](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/rest_api.html).

The client is generated with [Swagger Codegen](https://swagger.io/tools/swagger-codegen/) from an OpenAPI specification file.

## License

The library is distributed under the terms of BSD 3-Clause License.

    Copyright (c) 2019, Andrea Medeghini
    All rights reserved.
    
    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:
    
    * Redistributions of source code must retain the above copyright notice, this
      list of conditions and the following disclaimer.
    
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    
    * Neither the name of the library nor the names of its
      contributors may be used to endorse or promote products derived from
      this software without specific prior written permission.
    
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
    FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## How to get the binaries

The library is available in Maven Central Repository, Bintray, and GitHub.

If you are using Maven, add this dependency to your POM:

    <dependency>
        <groupId>com.nextbreakpoint</groupId>
        <artifactId>com.nextbreakpoint.flinkclient</artifactId>
        <version>1.0.2</version>
    </dependency>        

## How to build the library

Build the library using Maven:

    mvn clean package

## How to run the tests

Run the tests using Maven:

    mvn clean verify

## Documentation

Create the Flink client:

    FlinkApi api = new FlinkApi();

Configure host and port of the server:

    api.getApiClient().setBasePath("http://localhost:8081");

Configure socket timeouts:

    api.getApiClient().getHttpClient().setConnectTimeout(20000, TimeUnit.MILLISECONDS)
    api.getApiClient().getHttpClient().setWriteTimeout(30000, TimeUnit.MILLISECONDS)
    api.getApiClient().getHttpClient().setReadTimeout(30000, TimeUnit.MILLISECONDS)

Optionally enable debugging:

    api.getApiClient().setIsDebugging(true)

Get Flink cluster configuration:

    DashboardConfiguration config = api.showConfig();

Show list of uploaded jars:

    JarListInfo jars = api.listJars();

Upload a jar which contain a Flink job:

    JarUploadResponseBody result = api.uploadJar(new File("flink-job.jar"));

Run an uploaded jar which some arguments:

    JarRunResponseBody response = api.runJar("bf4afb3b-d662-435e-b465-5ddb40d68379_flink-job.jar", true, null, "--INPUT A --OUTPUT B", null, "your-main-class", null);

Get status of all jobs:

    JobIdsWithStatusOverview jobs = api.getJobs();

Get details of a job:

    JobDetailsInfo details = api.getJobDetails("f370f5421e5254eed8d6fc6673829c83");

Terminate a job:

    api.terminateJob("f370f5421e5254eed8d6fc6673829c83", "cancel");

For all the remaining operations see documentation of Monitoring REST API or see [OpenAPI specification file](https://github.com/nextbreakpoint/flink-client/blob/master/flink-openapi.yaml).

## Known limitations

The library has integration tests with a code coverage of 90%. Few endpoints don't have tests and not all fields in the responses are currently verified.
      
