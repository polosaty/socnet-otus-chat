import './App.css';
import React, { Component} from 'react'
import io from 'socket.io-client'


function getPrameter(name) {
  return new URLSearchParams(window.location.search).get(name)
}

// const DUMMY_DATA = [
//   {
//     id: getMsgId(),
//     senderId: "perborgen",
//     text: "who'll win?"
//   },
//   ...Array.from({ length: 15 }, (v, k) => {
//     return {
//       id: getMsgId(),
//       senderId: "janedoe",
//       text: `Test msg ${k}`
//     }
//   })
// ]

// function getCookie(name) {
//   let matches = document.cookie.match(new RegExp(
//     "(?:^|; )" + name.replace(/([.$?*|{}()[\]\\/+^])/g, '\\$1') + "=([^;]*)"
//   ));
//   return matches ? decodeURIComponent(matches[1]) : undefined;
// }

// function setCookie(name, value, options = {}) {

//   options = {
//     path: '/',
//     // при необходимости добавьте другие значения по умолчанию
//     ...options
//   };

//   if (options.expires instanceof Date) {
//     options.expires = options.expires.toUTCString();
//   }

//   let updatedCookie = encodeURIComponent(name) + "=" + encodeURIComponent(value);

//   for (let optionKey in options) {
//     updatedCookie += "; " + optionKey;
//     let optionValue = options[optionKey];
//     if (optionValue !== true) {
//       updatedCookie += "=" + optionValue;
//     }
//   }

//   document.cookie = updatedCookie;
// }


class MessageList extends Component {
  scrollElement() {
    const messageList = this.props.listRef.current
    messageList.scroll(0, messageList.scrollHeight)
  }

  componentDidMount() {
    this.scrollElement()
  }

  componentDidUpdate() {
    this.scrollElement()
  }

  render() {
    return (
      <ul className="message-list" ref={this.props.listRef} >
        {this.props.messages.map((message, index) => {
          const user = this.props.users && this.props.users[message.author_id] ? this.props.users[message.author_id] : null
          const class_name = message.currentUser ? "message my" : "message"
          const username = user ? `${user.firstname} ${user.lastname}` : message.author_id
          return (
            <li key={message.timestamp} className={class_name}>
              <div>{username}</div>
              <div>{message.content}</div>
            </li>
          )
        })}
      </ul>
    )
  }
}


class SendMessageForm extends Component {
  constructor() {
    super()
    this.state = {
      message: ''
    }
    this.handleChange = this.handleChange.bind(this)
    this.handleSubmit = this.handleSubmit.bind(this)
  }

  handleChange(e) {
    this.setState({
      message: e.target.value
    })
  }

  handleSubmit(e) {
    e.preventDefault()
    this.props.sendMessage({ messageText: this.state.message })
    this.setState({
      message: ''
    })
  }

  render() {
    return (
      <form
        onSubmit={this.handleSubmit}
        className="send-message-form">
        <input
          onChange={this.handleChange}
          value={this.state.message}
          placeholder="Type your message and hit ENTER"
          type="text" />
      </form>
    )
  }
}


function Title() {
  return <p className="title">Чат</p>
}

const SERVER_URL = 'http://127.0.0.1:8088'

class App extends Component {
  constructor(props) {
    super(props)


    this.state = {
      messages: [],
      users: {},
      userId: null,
      sio: null
    }

    this.messageListRef = React.createRef()
    this.sendMessage = this.sendMessage.bind(this)
  }

  sendMessage({ messageText }){
    if (!messageText) { return }
    if (!this.state.sio) { return }
    if (!this.state.userId) { return }

    this.state.sio.emit('message_add', {
      author_id: this.state.userId,
      content: messageText,
    // senderName
})

  }

  componentDidMount(){
    console.log('reconnect')
    const session = getPrameter('session')
    const chat_key = getPrameter('chat_key')
    const userId = parseInt(getPrameter('userId'))

    const sio = io(SERVER_URL, {
      query: { chat_key, session }
    })
    this.setState({ sio, userId })

    // const removeMessage = (id) => {
    //     sio.emit('message_remove', id)
    // }

    sio.on('connected', (message) => {
        this.setState({ users: message['users'] })
        sio.emit('message_get')
    })

    sio.on('disconnect', () => {
        sio.connect()
    })

    sio.on('users', (users) => {
        this.setState({ users })
    })

    sio.on('messages', (messages) => {
        const newMessages = messages.map((msg) =>
            msg.author_id && msg.author_id === this.state.userId ? { ...msg, currentUser: true } : msg
        )
        this.setState({messages: newMessages})

        sio.emit('messages_read', messages.map((m) => m.id))
    })

    sio.on('message', (message) => {
        const msg = (message.author_id && message.author_id === this.state.userId ? { ...message, currentUser: true } : message)
        this.setState({messages: [...this.state.messages, msg]})
        sio.emit('messages_read', [message.id])
    })

    sio.on('chat_response', (messages) => {
        console.log(messages)
    })
  }

  render() {
    return (
      <div className="app">
        <Title />
        <MessageList messages={this.state.messages} users={this.state.users} listRef={this.messageListRef} />
        <SendMessageForm
          sendMessage={this.sendMessage}
        />
      </div>
    )
  }
}




export default App;
