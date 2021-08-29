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
                }
            }
            data.push(payload)
        })
        return data;
    }
}

module.exports = new ATP;
