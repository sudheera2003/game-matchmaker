package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// JSON coming from rust
type ClientMessage struct {
	Action   string `json:"action"`
	PlayerID string `json:"player_id"`
}

// Map the Document going to MongoDB
type PlayerSession struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	PlayerID  string             `bson:"player_id"`
	Status    string             `bson:"status"`
	CreatedAt time.Time          `bson:"created_at"`
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	mongoURI := os.Getenv("ATLAS_URI")
	ctx := context.Background()
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("Mongo connection failed: ", err)
	}
	defer mongoClient.Disconnect(ctx)

	collection := mongoClient.Database("matchmaker").Collection("active_sessions")
	fmt.Println("Connected to MongoDB Atlas!")

	// Connect to Redis
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379" // Fallback for local testing
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed: ", err)
	}
	fmt.Println("Connected to Redis! Waiting for players...")

	// Worker Loop
	for {
		// BRPOP
		result, err := redisClient.BRPop(ctx, 0, "player_queue").Result()
		if err != nil {
			log.Println("Error reading from Redis:", err)
			continue
		}

		// result[0] is the queue name, result[1] is the actual JSON string from Rust
		rawJSON := result[1]

		// Parse the JSON into Go struct
		var msg ClientMessage
		if err := json.Unmarshal([]byte(rawJSON), &msg); err != nil {
			log.Println("Invalid JSON received:", err)
			continue
		}

		fmt.Printf("Processing %s for %s...\n", msg.Action, msg.PlayerID)

		filter := bson.M{"player_id": msg.PlayerID}

		update := bson.M{
			"$set": bson.M{
				"status":     "waiting_in_queue",
				"created_at": time.Now(),
			},
		}

		opts := options.Update().SetUpsert(true)

		_, err = collection.UpdateOne(ctx, filter, update, opts)
		if err != nil {
			log.Println("Failed to save to MongoDB:", err)
		} else {
			fmt.Printf("Successfully upserted %s in Atlas!\n", msg.PlayerID)

			// return broadcast
			// create a JSON payload that includes the player_id
			broadcastMsg := fmt.Sprintf(`{"player_id": "%s", "message": "Database confirmed: You are officially in the matchmaking queue!"}`, msg.PlayerID)

			// publish to the 'match_events' channel
			err = redisClient.Publish(ctx, "match_events", broadcastMsg).Err()
			if err != nil {
				log.Println("Failed to broadcast to Redis:", err)
			} else {
				fmt.Println("Broadcast sent to Rust servers!")
			}
		}
	}
}
