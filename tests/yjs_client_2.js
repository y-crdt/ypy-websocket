const Y = require('yjs')
const WebsocketProvider = require('y-websocket').WebsocketProvider

const doc = new Y.Doc()
const ytext = doc.getText('text')
const ws = require('ws')

const wsProvider = new WebsocketProvider(
  'ws://localhost:1234', 'my-roomname',
  doc,
  { WebSocketPolyfill: ws }
)

wsProvider.on('status', event => {
  console.log(event.status)
})

ytext.observe(event => {
  console.log(event)
})
