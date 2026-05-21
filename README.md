# pub-sub

An in-memory publish-subscribe broker via HTTP and Server-Sent Events (SSE), written in Go with no external dependencies.

## Prerequisites

- Go 1.25+

## Usage

```bash
go run main.go
```

Server listens on `:8000`.

## API

### Publish

Publish a message to a topic.

```http
POST /publish?topic=<topic>
Content-Type: text/plain

<message payload>
```

**Example:**

```bash
curl -X POST "http://localhost:8000/publish?topic=news" -d "Hello, World!"
# => ok
```

### Subscribe

Subscribe to a topic via SSE. The connection stays open and streams messages as they arrive.

```http
GET /subscribe?topic=<topic>
```

**Example:**

```bash
curl -N "http://localhost:8000/subscribe?topic=news"
# => data: Hello, World!
```

## Project Structure

```
.
├── go.mod       # Go module definition
├── main.go      # Broker, subscriber, message types, HTTP handlers, and entrypoint
└── README.md
```
