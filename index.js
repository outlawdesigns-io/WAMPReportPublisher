const autobahn = require('autobahn');
const mysql = require('mysql');

global.config = require('./config');
process.env.NODE_ENV = process.env.NODE_ENV || 'development';

const mysqlConn = mysql.createConnection({
  host: global.config[process.env.NODE_ENV].DBHOST,
  user: global.config[process.env.NODE_ENV].DBUSER,
  password: global.config[process.env.NODE_ENV].DBPASS
});

const wampConn = new autobahn.Connection({
  url:global.config[process.env.NODE_ENV].WAMPURL,
  realm:global.config[process.env.NODE_ENV].WAMPREALM
});

function _buildReportQueryStr(reportName,argCount){
  let retStr = `CALL ${global.config[process.env.NODE_ENV].DBDB}.${reportName}(`;
  for(let i = 0; i < argCount; i++){
    retStr += i == (argCount - 1) ? '?':'?,';
  }
  retStr += ')';
  return retStr;
}
function _getReports(mysqlConn){
  return new Promise((resolve,reject)=>{
    mysqlConn.query("SHOW PROCEDURE STATUS WHERE db = ?",[global.config[process.env.NODE_ENV].DBDB],(error,results,fields)=>{
      if (error) reject(error);
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
      if (error) reject(error);
      resolve(results[0].param_list);
    });
  });
}
function _callReport(mysqlConn,reportName,args){
  return new Promise((resolve,reject)=>{
    _getReportParams(mysqlConn,reportName).then((paramStr)=>{
      if(paramStr.split(',').length != args.length){
        reject('Mismatched argument length');
      }
      let queryStr = _buildReportQueryStr(reportName,args.length);
      mysqlConn.query(queryStr,args,(error,results,fields)=>{
        if(error) reject(error);
        resolve(results);
      });
    }).catch(reject);
  });
}


wampConn.onopen = async (session) => {
  console.log('connected to wamp router...');
  try{
    mysqlConn.connect();
    console.log('connected to mysql db...');
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

wampConn.open();

/*
periodically recall _getReports and register any new ones.
*/

setTimeout(()=>{
  wampConn.session.call('io.outlawdesigns.loe.music.rpt_PlayedSong_FirstTimeAndAllToDate',['wtd']).then(console.log).catch(console.error);
},3000);
