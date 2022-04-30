const Y = require('yjs')
const ws = require('ws')
const WebsocketProvider = require('y-websocket').WebsocketProvider

const ydoc = new Y.Doc()
const ymap = ydoc.getMap("map")
const wsProvider = new WebsocketProvider(
  'ws://localhost:1234', 'my-roomname',
  ydoc,
  { WebSocketPolyfill: ws }
)

wsProvider.on('status', event => {
  console.log(event.status)
})

ymap.observe(ymapEvent => {
  ymapEvent.changes.keys.forEach((change, key) => {
    if (key === 'clock') {
      const clock = ymap.get('clock')
      if (clock == 0) {
        const ytext = ymap.get('text')
        ydoc.transact(() => {
          ytext.insert(0, 'i')
          ymap.set('clock', 1)
        })
      }
    }
  })
})
