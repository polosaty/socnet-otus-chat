import { useEffect, useRef, useState } from 'react'
import io from 'socket.io-client'
// import { nanoid } from 'nanoid'
// hooks
// import { useLocalStorage, useBeforeUnload } from 'hooks'
import { useBeforeUnload } from '.'

const SERVER_URL = 'http://127.0.0.1:8080'

export const useChat = (roomId, addMessage, setMessages) => {
    const [users, setUsers] = useState([])
    // const [messages, setMessages] = useState([])

    // const [userId] = useLocalStorage('userId', nanoid(8))
    // const [username] = useLocalStorage('username')
    const userId = 1;
    const username = 'username'


    const socketRef = useRef(null)


    useEffect(() => {

        console.log('reconnect')
        socketRef.current = io(SERVER_URL, {
            query: { roomId }
        })


        socketRef.current.on('connected', (message) => {
            socketRef.current.emit('user_add', { username, userId })
            socketRef.current.emit('message_get')
        })


        socketRef.current.on('users', (users) => {
            setUsers(users)
        })


        socketRef.current.on('messages', (messages) => {
            const newMessages = messages.map((msg) =>
                msg.authorId && msg.authorId === userId ? { ...msg, currentUser: true } : msg
            )
            setMessages(newMessages)
        })

        socketRef.current.on('message', (message) => {
            addMessage(message, userId)
        })

        socketRef.current.on('chat_response', (messages) => {
            // const newMessages = messages.map((msg) =>
            //     msg.userId && msg.userId === userId ? { ...msg, currentUser: true } : msg
            // )
            // setMessages(newMessages)
            console.log(messages)
        })

        // return () => {
        //     socketRef.current.disconnect()
        // }
    }, [roomId, userId, username, addMessage, setMessages])

    const sendMessage = ({ messageText }) => {
        socketRef.current.emit('message_add', {
            authorId: userId,
            content: messageText,
            // senderName
        })
    }

    const removeMessage = (id) => {
        socketRef.current.emit('message_remove', id)
    }

    useBeforeUnload(() => {
        socketRef.current.emit('user_leave', userId)
    })

    return { users, sendMessage, removeMessage }
}
