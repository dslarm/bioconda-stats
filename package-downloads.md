----
# Directory structure
```
package-downloads/
                  anaconda.org/
                               <channel>.json
                               <channel>/
                                         <package>.json
                                         <package>/
                                                   <version>.json
                                                   <version>/
                                                             <subdir>.json
                                                             <subdir>/
                                                                      <basename>.json
```

----
# JSON structure
```
// package-downloads/anaconda.org/channel.json
{
    "channel": "<channel>",
    "download_per_package": [
        {"package": "<package-name>", "total": <total>},
        ...
    ]
    "downloads_per_date": [
        {"date": "YYYY-mm-dd", "total": <total>},
        ...
    ]
]
```
```
// package-downloads/anaconda.org/channel/package.json
{
    "channel": "<channel>",
    "package": "<package-name>",
    "downloads_per_version": [
        {"version": "<package-version>", "total": <total>},
        ...
    ]
    "downloads_per_date": [
        {"date": "YYYY-mm-dd", "total": <total>},
        ...
    ]
}
```
```
// package-downloads/anaconda.org/channel/package/version.json
{
    "channel": "<channel>",
    "package": "<package-name>",
    "version": "<package-version>",
    "downloads_per_subdir": [
        {"subdir": "<build-subdir>", "total": <total>},
        ...
    ]
    "downloads_per_date": [
        {"date": "YYYY-mm-dd", "total": <total>},
        ...
    ]
}
```
```
// package-downloads/anaconda.org/channel/package/version/subdir.json
{
    "channel": "<channel>",
    "package": "<package-name>",
    "version": "<package-version>",
    "subdir": "<build-subdir>",
    "downloads_per_basename": [
        {"basename": "<build-basename>", "total": <total>},
        ...
    ]
    "downloads_per_date": [
        {"date": "YYYY-mm-dd", "total": <total>},
        ...
    ]
}
```
```
// package-downloads/anaconda.org/channel/package/version/subdir/basename.json
{
    "channel": "<channel>",
    "package": "<package-name>",
    "version": "<package-version>",
    "subdir": "<build-subdir>",
    "basename": "<build-basename>",
    "downloads_per_date": [
        {"date": "YYYY-mm-dd", "total": <total>},
        ...
    ]
}
```

----
# Example

```
// package-downloads/anaconda.org/bioconda.json
{
"channel":"bioconda",
"downloads_per_package":[
// Limited to top N packages to avoid bloating the channel.json
{"package":"example","total":23456}
],
"downloads_per_date":[
{"date":"2021-01-01","total":21234},
// For stretches without download count changes, only record start and end points:
{"date":"2021-01-02","total":22345},
// {"date":"2021-01-03","total":22345}, // Same count as previous and next date => exclude from .json
{"date":"2021-01-04","total":22345},  // Same count as previous but not next date => include as end point
{"date":"2021-01-05","total":23456}
]
}
```
```
// package-downloads/anaconda.org/bioconda/example.json
{
"channel":"bioconda",
"name":"example-package",
"downloads_per_date":[
{"date":"2021-01-01","total":21234},
// For stretches without download count changes, only record start and end points:
{"date":"2021-01-02","total":22345},
// {"date":"2021-01-03","total":22345}, // Same count as previous and next date => exclude from .json
{"date":"2021-01-04","total":22345},  // Same count as previous but not next date => include as end point
{"date":"2021-01-05","total":23456}
]
}
```
```
// package-downloads/anaconda.org/bioconda/example/1.0.0.json
{
"channel":"bioconda",
"name":"example-package",
"version":"1.0.0",
"downloads_per_date":[
{"date":"2021-01-01","total":21234},
// For stretches without download count changes, only record start and end points:
{"date":"2021-01-02","total":22345},
// {"date":"2021-01-03","total":22345}, // Same count as previous and next date => exclude from .json
// {"date":"2021-01-04","total":22345}, // Same count as previous and next date => exclude from .json
{"date":"2021-01-05","total":23456}
]
}
```
```
// package-downloads/anaconda.org/bioconda/example/1.0.0/linux-64.json
{
"channel":"bioconda",
"name":"example-package",
"version":"1.0.0",
"subdir":"linux-64",
"downloads_per_date":[
{"date":"2021-01-01","total":11234},
// For stretches without download count changes, only record start and end points:
{"date":"2021-01-02","total":12345},
// {"date":"2021-01-03","total":12345}, // Same count as previous and next date => exclude from .json
// {"date":"2021-01-04","total":12345}, // Same count as previous and next date => exclude from .json
{"date":"2021-01-05","total":13456}
]
}
```
```
// package-downloads/anaconda.org/bioconda/example/1.0.0/linux-64/example-1.0.0-h1234567_1.tar.bz2.json
{
"channel":"bioconda",
"name":"example-package",
"version":"1.0.0",
"subdir":"linux-64",
"basename":"example-1.0.0-h1234567_1.tar.bz2",
"downloads_per_date":[
{"date":"2021-01-01","total":1234},
// For stretches without download count changes, only record start and end points:
{"date":"2021-01-02","total":2345},
// {"date":"2021-01-03","total":2345}, // Same count as previous and next date => exclude from .json
// {"date":"2021-01-04","total":2345}, // Same count as previous and next date => exclude from .json
{"date":"2021-01-05","total":3456}
]
}
```
