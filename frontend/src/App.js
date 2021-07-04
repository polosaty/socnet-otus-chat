import './App.css';
import React, { Component, useState} from 'react'
// import { io } from "socket.io-client";
import { useChat } from './hooks'
// import logo from './logo.svg';


let _id = 1
function getMsgId() { return _id++ }


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

// const DUMMY_DATA = io.

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
          return (
            <li key={message.id || `msg-id-${getMsgId()}`} className="message">
              <div>{message.authorId}</div>
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


// class App extends Component {
//   constructor() {
//     super()


//     this.state = {
//       messages: messages,
//       users: users
//     }
//     // this.hooks = {
//     //   sendMessage: sendMessage,
//     //   removeMessage: removeMessage
//     // }

//     this.messageListRef = React.createRef()
//     this.sendMessage = this.sendMessage.bind(this)
//   }

//   sendMessage(msg){
//     if (!msg) { return }

//     const message = {
//       senderId: "janedoe",
//       text: msg
//     }
//     this.setState({
//       messages: [...this.state.messages, message]
//     })

//     // const messageList = this.messageListRef.current
//     // messageList.scroll(0, messageList.scrollHeight)
//   }

//   render() {
//     const roomId = 1
//     const { users, messages, sendMessage, removeMessage } = useChat(roomId)
//     return (
//       <div className="app">
//         <Title />
//         <MessageList messages={this.state.messages} listRef={this.messageListRef} />
//         <SendMessageForm
//           sendMessage={this.sendMessage}
//         />
//       </div>
//     )
//   }
// }
const App = () => {

    // super()


    // this.state = {
    //   messages: messages,
    //   users: users
    // }
    // // this.hooks = {
    //   sendMessage: sendMessage,
    //   removeMessage: removeMessage
    // }

    //   this.messageListRef = React.createRef()
    //   this.sendMessage = this.sendMessage.bind(this)
    // }

    // sendMessage(msg){
    //   if (!msg) { return }

    //   const message = {
    //     senderId: "janedoe",
    //     text: msg
    //   }
    //   this.setState({
    //     messages: [...this.state.messages, message]
    //   })

    //   // const messageList = this.messageListRef.current
    //   // messageList.scroll(0, messageList.scrollHeight)
    // }

    const [messages, setMessages] = useState([])
    const addMessage = (message, userId) => {
      const newMessages = [...messages,
      message.authorId && message.authorId === userId ?
        { ...message, currentUser: true } : message
      ]
      setMessages(newMessages)
    }
    const roomId = 1;
    // const { users, messages, sendMessage, removeMessage } = useChat(roomId)
    const { users, sendMessage, removeMessage } = useChat(roomId, addMessage, setMessages)
    // console.log(users, removeMessage)
    const messageListRef = React.createRef()
    return (
      <div className="app">
        <Title />
        <MessageList
          messages={messages}
          removeMessage={removeMessage}
          listRef={messageListRef} />
        <SendMessageForm
          sendMessage={sendMessage}
          users={users}
        />
      </div>
    )

}


export default App;
