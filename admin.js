const { kafka } = require('./client')

async function init() {
    const admin = kafka.admin()
    await admin.connect()
    console.log("Admin connected")
    await admin.createTopics({
        topics: [
            { topic: 'rider-updates', numPartitions: 2 }
        ]
    })
    console.log("Topic created")
    await admin.disconnect()
    console.log("Admin disconnected")
}

init().catch(console.error)