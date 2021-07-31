# ATE (AWS Timestream Easy)

ATE is a Node library for simplifying AWS Timestream operations.

## Installation

Use the package manager [npm](https://npmjs.com/) to install ATE.

```bash
npm install @mattnick/aws-timestream-easy
```

## Usage

```javascript
const ate = require('@mattnick/aws-timestream-easy');

async function test() {
  const query_string = 'SELECT * FROM "weather-DB"."weather-logs" ORDER BY time DESC LIMIT 10';
  const response = await ate.query(query_string);
  console.log(response);
}

test();
/*
response {
  error: null,
  results: [...]
}
*/

```
#### Results Structure
Result structures include any dimensions you set when writing the record to timestream. The data format of the measure_value is whatever you specified when writing the record to timestream.

```json
[
  {
    "eventID": "3e384710-f19e-11eb-9a03-0242ac130003",
    "measure_name": "temperature",
    "measure_value": 98.6,
    "time": 1609462801,
    ...
  },
  {
    "eventID": "3e384710-f19e-11eb-9a03-0242ac130003",
    "measure_name": "wind_direction",
    "measure_value": "SSE",
    "time": 1609462801
  }
  ...
]
```

#### Supported Data Types
- VARCHAR
- INT
- BIGINT
- DOUBLE
- TIMESTAMP
- BOOLEAN