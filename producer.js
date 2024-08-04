const { kafka } = require('./client')
const readline = require('readline')

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
})

async function init() {
    const producer = kafka.producer()
    await producer.connect()
    console.log("Producer connected")

    rl.setPrompt('> ')
    rl.prompt()

    rl.on('line', async function(line){
        const [riderName, location] = line.split(' ')

        await producer.send({
            topic: 'rider-updates',
            messages: [
                {
                    partition: location.toLocaleLowerCase() === 'north' ? 0 : 1, 
                    key: 'locationUpdate', 
                    value: JSON.stringify({
                        name: riderName,
                        location: location
                    })
                },
            ],
        })
        console.log("Message sent")
    }).on('close', async () => {
        await producer.disconnect()
        console.log("Producer disconnected")
    })
}

init().catch(console.error)