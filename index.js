const autobahn = require('autobahn');
const mysql = require('mysql');

global.config = require('./config');
process.env.NODE_ENV = process.env.NODE_ENV || 'development';

// const mysqlConn = mysql.createConnection({
//   host: global.config[process.env.NODE_ENV].DBHOST,
//   user: global.config[process.env.NODE_ENV].DBUSER,
//   password: global.config[process.env.NODE_ENV].DBPASS
// });

const mysqlConn = mysql.createPool({
  host: global.config[process.env.NODE_ENV].DBHOST,
  user: global.config[process.env.NODE_ENV].DBUSER,
  password: global.config[process.env.NODE_ENV].DBPASS
});

const wampConn = new autobahn.Connection({
  url:global.config[process.env.NODE_ENV].WAMPURL,
  realm:global.config[process.env.NODE_ENV].WAMPREALM
});

function _buildReportQueryStr(reportName,argCount){
  if(argCount < 0){
    throw new Error('Cannot support negative argument count');
  }
  let retStr = `CALL ${global.config[process.env.NODE_ENV].DBDB}.${reportName}`;
  if(argCount != 0){
      retStr += '(';
  }
  for(let i = 0; i < argCount; i++){
    retStr += i == (argCount - 1) ? '?':'?,';
  }
  if(argCount != 0){
      retStr += ')';
  }
  return retStr;
}
function _getReports(mysqlConn){
  return new Promise((resolve,reject)=>{
    mysqlConn.query("SHOW PROCEDURE STATUS WHERE db = ?",[global.config[process.env.NODE_ENV].DBDB],(error,results,fields)=>{
      if (error) return reject(error);
      if (results === undefined) return reject(new Error('No Stored Procedures'));
      resolve(results.filter((e)=>{
        return e.Name.startsWith(global.config[process.env.NODE_ENV].DBRPTPREFIX);
      }).map((e)=>{
        return e.Name
      }));
    });
  });
}
function _getReportParams(mysqlConn,reportName){
  return new Promise((resolve,reject)=>{
    mysqlConn.query("SELECT CONVERT(param_list USING utf8) as param_list FROM mysql.proc WHERE db=? AND name=?",[global.config[process.env.NODE_ENV].DBDB,reportName],(error,results,fields)=>{
      if (error) return reject(error);
      resolve(results[0].param_list);
    });
  });
}
function _callReport(mysqlConn,reportName,args){
  return new Promise((resolve,reject)=>{
    _getReportParams(mysqlConn,reportName).then((paramStr)=>{
      let queryStr;
      if(paramStr != '' && paramStr.split(',').length != args.length){
         return reject('Mismatched argument length');
      }
      queryStr = _buildReportQueryStr(reportName,args.length);
      mysqlConn.query(queryStr,args,(error,results,fields)=>{
        if(error) return reject(error);
        resolve(results);
      });
    }).catch(reject);
  });
}


wampConn.onopen = async (session) => {
  console.log('connected to wamp router...');
  try{
    let reports = await _getReports(mysqlConn);
    reports.forEach(async (e)=>{
      let functionName = `${global.config[process.env.NODE_ENV].RPCPREFIX}.${e}`;
      await session.register(functionName, async (args)=>{ return await _callReport(mysqlConn,e,args) });
      console.log(`${functionName} registered...`);
    });
  }catch(err){
    console.error(err);
  }
}

mysqlConn.on('error',(error)=>{
  console.log('caught error on mysqlConn...');
  console.error(error);
});

wampConn.open();

/*
periodically recall _getReports and register any new ones.
implement an incremental backoff on mysqlconn error <-- using pool may have fixed this
*/

setTimeout(()=>{
  wampConn.session.call('io.outlawdesigns.loe.music.rpt_PlayedSong_FirstTimeAndAllToDate',['wtd']).then(console.log).catch(console.error);
},3000);
