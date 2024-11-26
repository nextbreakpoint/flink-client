# flink-client

This library provides a Java client for managing Apache Flink via the [REST API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/rest_api/).

The client is generated with [Swagger Codegen](https://swagger.io/tools/swagger-codegen/) from the 
[OpenAPI specification](https://nightlies.apache.org/flink/flink-docs-release-1.20/generated/rest_v1_dispatcher.yml).

## License

The library is distributed under the terms of BSD 3-Clause License.

    Copyright (c) 2019-2024, Andrea Medeghini
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

The library is available in Maven Central Repository, and GitHub.

If you are using Maven, add this dependency to your POM:

    <dependency>
        <groupId>com.nextbreakpoint</groupId>
        <artifactId>com.nextbreakpoint.flink.client</artifactId>
        <version>1.1.1</version>
    </dependency>        

## How to generate the code

Generate the code using Maven:

    mvn clean compile

## How to build the library

Build the library using Maven:

    mvn clean package

## How to run the tests

Make sure you have Docker installed and create the required bridge network:

    docker network create flink-test 
 
Run the tests using Maven:

    mvn clean verify

## Documentation

Create the Flink client:

    FlinkApi api = new FlinkApi();

Configure host and port of the server:

    api.getApiClient().setBasePath("http://localhost:8081");

Configure socket timeouts:

    api.getApiClient().setHttpClient(api.getApiClient().getHttpClient().newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .writeTimeout(Duration.ofSeconds(30))
            .readTimeout(Duration.ofSeconds(30))
            .build());

Optionally enable debugging:

    api.getApiClient().setDebugging(true);

Get Flink cluster configuration:

    DashboardConfiguration config = api.getDashboardConfiguration();

Show list of uploaded jars:

    JarListInfo jars = api.getJarList();

Upload a jar which contain a Flink job:

    JarUploadResponseBody result = api.uploadJar(new File("flink-job.jar"));

Run an uploaded jar which some arguments:

    JarRunResponseBody response = api.submitJobFromJar("bf4afb3b-d662-435e-b465-5ddb40d68379_flink-job.jar", true, null, null, "--INPUT A --OUTPUT B", "your-main-class", null);

Get status of all jobs:

    JobIdsWithStatusOverview jobs = api.getJobIdsWithStatusesOverview();

Get details of a job:

    JobDetailsInfo details = api.getJobDetails("f370f5421e5254eed8d6fc6673829c83");

Terminate a job:

    api.cancelJob("f370f5421e5254eed8d6fc6673829c83", TerminationMode.CANCEL);

For all the remaining operations see the documentation of Flink's [REST API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/rest_api/).

## Flink compatibility

The library is compatible with Flink Monitoring REST API v1.

The current version of the library has been tested against Flink 1.20.0, but the library is known to work with older versions too.   
The library should be compatible with newer versions too, unless a breaking change is introduced in the Flink Monitoring REST API. 

## Known limitations

Some endpoints haven't been tested and not all fields in the responses are currently verified in the tests.
 
