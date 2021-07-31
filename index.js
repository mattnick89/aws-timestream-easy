function unmarshall(res) {
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