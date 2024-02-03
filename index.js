
const oracleInstantClient = require("oracle-instantclient");

let instantClientPath = oracleInstantClient.path // path to the binaries
const oracledb = require('oracledb');

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

let queueName = 'C##dequeuer.QUEUE_NAME';
let dbUser = 'SYSTEM';
let dbPassword = 'admin';
let connectString = 'localhost/FREE';
oracledb.initOracleClient({
    libDir: instantClientPath,
});
let pool = null;
let eventsPool = null;

async function run() {

  pool = await oracledb.createPool({
    user: dbUser,
    password: dbPassword,
    connectString: connectString,
  });

  const connection = await pool.getConnection();
  // const queue = await connection.getQueue(queueName, { payloadType: oracledb.JMS});
  const jmsMessageType = "SYS.AQ$_JMS_TEXT_MESSAGE";
  const queue = await connection.getQueue(queueName, {payloadType: jmsMessageType});

  let response;
  try {
    let request = 'hello world';
      const enqmsg = new queue.payloadTypeClass(
            {
                HEADER: {
                    USERID: "me",
                    PROPERTIES: [
                        {
                            NAME: "JMS_OracleDeliveryMode",
                            TYPE: 100,
                            STR_VALUE: "2",
                            NUM_VALUE: null,
                            JAVA_TYPE: 27
                        },
                        {
                            NAME: "JMS_OracleTimestamp",
                            TYPE: 200,
                            STR_VALUE: null,
                            NUM_VALUE: new Date().getTime(),
                            JAVA_TYPE: 24
                        }
                    ]
                },
                TEXT_LEN: request.length,
                TEXT_VC: request
            }
        );
      response = await queue.enqOne(enqmsg);
      // response = await queue.enqOne({payload: 'hola mamita', priority: 1, numAttempts: 3, });
      connection.commit();
  }
  catch(e) {
      console.error(`Error enqueuing: '${e}'`);
  }

  queue.deqOptions.wait = oracledb.AQ_DEQ_NO_WAIT;

  let deq;
  try {

    deq = await queue.deqOne();
    console.log(`dequeued msg is ${deq}`);
    console.log({deq})
    console.log(deq.priority, deq.numAttempts);
    console.log(deq.payload.toString())
  }
  catch(e) {
    console.error(`Error dequeueing: '${e}'`);
  }
  finally {
      await connection.commit();
      await connection.close();
  }
  return response;

  // const connection = await oracledb.getConnection ({
  //     user          : "SYSTEM",
  //     password      : "admin",
  //     connectString : "localhost/FREE"
  // });

  // let queue = connection.getQueue('C##dequeuer.QUEUE_NAME');
  // const msg = await queue.enqOne('hello wolrd');
  // await connection.commit();

  // const result = await connection.execute(`SELECT * FROM test_oracle`);

  // console.log(result.rows);
  // await connection.close();
}

run();