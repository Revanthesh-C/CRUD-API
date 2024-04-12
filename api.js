
const { text } = require('body-parser');
const client = require('./connect.js')
const express = require('express')
const exp = express();

exp.use(express.json())

exp.listen(3300,()=>{
    console.log('Server is listening at port 3300')
})

client.connect()

function client_side_display(id,err,status,errmsg,res_id)
{
      text = `{
        "id": ${params.id},
        "ver": "1.0",
        "ts": ${Date.new()},
        "params": {
          "err": ${params.err},
          "status": ${params.successful},
          "errmsg": ${params.errmsg}
        },
        "responseCode": "OK",
        "result": {
          "id": ${params.res_id}
        }
      }`

      return text;
}

exp.get('/v1/datasets/:id',(req,res)=>{
    client.query(`select * from datasets where id = '${req.params.id}'`,(err,result)=>{
        if(!err)
        {
              if(result.rows.length==0)
              res.status(400).send('Enter proper route')
              else 
              res.status(200).send(result.rows)
        }
        
        else
        {
          res.status(500).send(err.message)
        }
        })
    client.end;
})

exp.get('/v1/datasets/',(req,res) => {
    client.query(`select * from datasets`,(err,result) => {
        if(!err)
        res.status(200).send(result.rows)
        else
        {
            res.status(500).send(err.message)
        }
        })
    client.end;
})

exp.post('/v1/datasets',(req,res)=>{
    const idata = req.body;
    const keys = Object.keys(idata)
    const values = Object.values(idata);
    const missingvals=[];
    if(!idata.id)
    missingvals.push('id') 
    if(!idata.type)
    missingvals.push('type') 
    if(!idata.updated_date)
    missingvals.push('updated_date')

  
    if(missingvals.length>0)
        res.status(417).send(`${missingvals.join(",")} Insertion of these fields is compulsory`)
    else{
    const query = `INSERT INTO datasets (${keys.join(",")}) VALUES (${keys.map((key, index) => `$${index + 1}`).join(",")}) RETURNING *` 

    client.query(query, values,(err,result)=>{
        if(!err)
        res.status(200).send({
            "id": "api.dataset.create",
            "ver": "1.0",
            "ts": "2024-04-10T11:20:12ZZ",
            "params": {
              "err": null,
              "status": "successful",
              "errmsg": null
            },
            "responseCode": "OK",
            "result": {
              "id": idata.dataset_id
            }
          })
        else
        res.status(500).send({
            "id": "api.dataset.create",
            "ver": "1.0",
            "ts": "2024-04-10T11:20:12ZZ",
            "params": {
              "err": 'true',
              "status": "unsuccessful",
              "errmsg": `${err.message}`
            },
            "responseCode": "OK",
          })
    })
    client.end;
  }
})



exp.put('/v1/datasets/:dataset_id', (req,res)=>{
    const udata = req.body;
    const keys = Object.keys(udata)
    const update =client.query('update datasets set id = $1 ,type = $2 ,name = $3 ,validation_config = $4 ,extraction_config  = $5 ,dedup_config = $6  ,  data_schema = $7  ,denorm_config = $8  , router_config = $9  , dataset_config = $10  ,status = $11  ,tags = $12  ,data_version = $13  ,created_by = $14  ,updated_by = $15  ,created_date = $16  ,updated_date = $17  ,published_date = $18 WHERE dataset_id = $19',
                    [udata.id ,udata.type ,udata.name ,udata.validation_config ,udata.extraction_config ,udata.dedup_config ,  udata.data_schema ,udata.denorm_config , udata.router_config , udata.dataset_config ,udata.status ,udata.tags ,udata.data_version ,udata.created_by ,udata.updated_by ,udata.created_date ,udata.updated_date ,udata.published_date,`${req.params.dataset_id}`],
                    (err,result) => {
                        if(!err)
                        {
                            if(result.rowCount==0)
                            {
                              res.status(400).send({"id": "api.dataset.update",
                                "ver": "1.0",
                                "ts": "2024-04-10T11:20:12ZZ",
                                "params": {
                                  "err": 'true',
                                  "status": "unsuccessful",
                                  "errmsg": "Enter proper Route"
                                },
                                "responseCode": "OK"
                              })

                            }
                            else
                            {
                            res.status(200).send({"id": "api.dataset.update",
                            "ver": "1.0",
                            "ts": "2024-04-10T11:20:12ZZ",
                            "params": {
                              "err": null,
                              "status": "successful",
                              "errmsg": null
                            },
                            "responseCode": "OK",
                            "result": {
                              "id": `${req.params.id}`
                            }})
                          }
                        }
                        else
                            res.status(500).send({
                                "id": "api.dataset.update",
                                "ver": "1.0",
                                "ts": "2024-04-10T11:20:12ZZ",
                                "params": {
                                  "err": 'true',
                                  "status": "unsuccessful",
                                  "errmsg": `${err.message}`
                                },
                                "responseCode": "OK",
                              })
                    })
    client.end;
})

exp.patch('/datasets/:dataset_id',async (req,res)=>{
  const udata = req.body;
  const keys = Object.keys(udata)
  const value = Object.values(udata)
  for(i=0;i<keys.length;i++)
  {
  const update = await client.query(`update datasets set ${keys[i]} = $1 where dataset_id = $2`,[`${value[i]}`,`${req.params.dataset_id}`],
              (err,result) => {
                  if(!err)
                  {
                    if(result.rowCount==0)
                    res.status(400).send('No rows are updated')
                  }
                  else
                  {
                      res.send({
                          "id": "api.dataset.update",
                          "ver": "1.0",
                          "ts": "2024-04-10T11:20:12ZZ",
                          "params": {
                            "err": 'true',
                            "status": "unsuccessful",
                            "errmsg": `${err.message}`
                          },
                          "responseCode": "OK",
                        })
                  }      
              })
              if(i==keys.length-1)
              {
                  res.send({"id": "api.dataset.update",
                  "ver": "1.0",
                  "ts": "2024-04-10T11:20:12ZZ",
                  "params": {
                    "err": null,
                    "status": "successful",
                    "errmsg": null
                  },
                  "responseCode": "OK",
                  "result": {
                    "id": udata.dataset_id
                  }
                  })
                  client.end;
              }
              
  }            
})  
  


                   /* if(!err)
                    {
                        if(result.rowCount==0)
                            {
                              res.send({"id": "api.dataset.update",
                                "ver": "1.0",
                                "ts": "2024-04-10T11:20:12ZZ",
                                "params": {
                                  "err": 'true',
                                  "status": "unsuccessful",
                                  "errmsg": "Enter proper Route"
                                },
                                "result": {
                              "id": `${req.params.id}`
                              }})
                            }
                                            
                        else
                        {
                        res.send({
                            "id": "api.dataset.update",
                            "ver": "1.0",
                            "ts": "2024-04-10T11:20:12ZZ",
                            "params": {
                              "err": 'true',
                              "status": "unsuccessful",
                              "errmsg": `${err.message}`
                            },
                            "responseCode": "OK",
                          })
                       }      
                }
                if(i==keys.length-1)
                {
                    res.send({"id": "api.dataset.update",
                    "ver": "1.0",
                    "ts": "2024-04-10T11:20:12ZZ",
                    "params": {
                      "err": null,
                      "status": "successful",
                      "errmsg": null
                    },
                    "responseCode": "OK",
                    "result": {
                      "id": req.params.dataset_id
                    }
                    })
                    client.end;
                }
              })*/    

exp.delete('/datasets/:dataset_id',(req,res)=>{
    client.query(`delete from datasets where dataset_id='${req.params.dataset_id}'`,(err,result)=>{
        if(!err)
        res.status(200).send('Deleted successfully')
        else
        {
            res.send('page not found')
        }
        })
    client.end;
})
