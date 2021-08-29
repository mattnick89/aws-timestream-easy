const AWS = require('aws-sdk');
const https = require('https');
const { DateTime } = require('luxon');
const agent = new https.Agent({
    maxSockets: 5000
});
const writeClient = new AWS.TimestreamWrite({
    maxRetries: 10,
    httpOptions: {
        timeout: 20000,
        agent: agent
    }
});
const queryClient = new AWS.TimestreamQuery();

class ATP {

    query(query_string){
        return new Promise(async (resolve)=>{
            let data = [];
            let qr = true;
            let error = null;
            while(qr){
                const d = await this._query(query_string);
                if(d.res){
                    data.push(d.res)
                }
                if(d.error){ error = d.error; qr = false; }
                if(!d.next){ qr = false; }
            }
            return resolve({error: error, results: data})
        })
    }

    _query(query_string){ 
        return new Promise((resolve)=>{
            queryClient.query({QueryString: query_string}).promise()
            .then((res)=>resolve({res: this.parse(res), next: (res.NextToken) ? res.NextToken : null, error: null}))
            .catch((err)=>resolve({error: err}))
        })
    }

    parse(res) {
        let data = [];
        res.Rows.forEach((row)=>{
            let payload = {};
            for(let i=0; i<res.ColumnInfo.length; ++i){
                let val = null;
                if(row.Data[i]['ScalarValue']){
                    if(res.ColumnInfo[i].Name.split("::")[0] == 'measure_value'){
                        switch(res.ColumnInfo[i].Type['ScalarType'].toUpperCase()){
                            case "BOOLEAN":
                                val = (row.Data[i]['ScalarValue'] == 'true') ? true : false;
                            break;
                            case "DOUBLE":
                                val = parseFloat(""+row.Data[i]['ScalarValue'])
                            break;
                            case "VARCHAR":
                                val = row.Data[i]['ScalarValue']
                            break;
                            case "BIGINT":
                                val = parseInt(""+row.Data[i]['ScalarValue'])
                            break;
                            case "INT":
                                val = parseInt(""+row.Data[i]['ScalarValue'])
                            break;
                            case "TIMESTAMP":
                                let ts = row.Data[i]['ScalarValue'].replace(' ', "T");
                                val = Math.floor(DateTime.fromISO(ts).toSeconds())
                            break;
                        }
                        payload.measure_value = val;
                    }else {
                        switch(res.ColumnInfo[i].Type['ScalarType'].toUpperCase()){
                            case "BOOLEAN":
                                val = (row.Data[i]['ScalarValue'] == 'true') ? true : false;
                            break;
                            case "DOUBLE":
                                val = parseFloat(""+row.Data[i]['ScalarValue'])
                            break;
                            case "VARCHAR":
                                val = row.Data[i]['ScalarValue']
                            break;
                            case "BIGINT":
                                val = parseInt(""+row.Data[i]['ScalarValue'])
                            break;
                            case "INT":
                                val = parseInt(""+row.Data[i]['ScalarValue'])
                            break;
                            case "TIMESTAMP":
                                let ts = row.Data[i]['ScalarValue'].replace(' ', "T");
                                val = Math.floor(DateTime.fromISO(ts).toSeconds())
                            break;
                        }   
                        payload[res.ColumnInfo[i].Name] = val;
                    }
                } else if(row.Data[i]['TimeSeriesValue']){ 
                    let timeseriesdata = [];
                    for(let z=0; z<row.Data[i]['TimeSeriesValue'].length; z++){
                         let rowresult = {};
                         rowresult.time = row.Data[i]['TimeSeriesValue'][z].Time;
                         if(this.isBoolean(row.Data[i]['TimeSeriesValue'][z].Value.ScalarValue)){               // detect if Number or boolean or varchar, timeseries can only be bigint, boolean, double, or varchar
                             rowresult.value = (row.Data[i]['TimeSeriesValue'][z].Value.ScalarValue == 'true') ? true : false;
                         }  else if(Number(row.Data[i]['TimeSeriesValue'][z].Value.ScalarValue)!=NaN){          // Try to parse it as a number. If this fails, its a varchar
                             rowresult.value = Number(""+row.Data[i]['TimeSeriesValue'][z].Value.ScalarValue);
                         } else {
                            rowresult.value = row.Data[i]['TimeSeriesValue'][z].Value.ScalarValue;              // if we can't parse it as a bool or a number, its a varchar.
                          }
                         timeseriesdata.push(rowresult);
                     }
                    payload[res.ColumnInfo[i].Name] = timeseriesdata;                                           // Not sure how create_time_series handles query boundaries. This will fail on data.push if timeseries query can go over query token boundary.
                }
            }
            data.push(payload)
        })
        return data;
    }

    isBoolean(val) {
        return val === false || val === true;
     }
}

module.exports = new ATP;
