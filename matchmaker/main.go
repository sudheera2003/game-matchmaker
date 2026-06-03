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
	Action      string `json:"action"`
	PlayerID    string `json:"player_id"`
	SkillRating int    `json:"skill_rating"
}

// Map the Document going to MongoDB
type PlayerSession struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	PlayerID    string             `bson:"player_id"`
	SkillRating int                `bson:"skill_rating"`
	Rank        string             `bson:"rank"`
	Status      string             `bson:"status"`
	CreatedAt   time.Time          `bson:"created_at"`
}

// Calculate rank tier based on raw MMR
func getRank(mmr int) string {
	if mmr <= 1000 {
		return "Bronze"
	}
	if mmr <= 2000 {
		return "Silver"
	}
	if mmr <= 3000 {
		return "Gold"
	}
	if mmr <= 4000 {
		return "Platinum"
	}
	if mmr <= 5000 {
		return "Diamond"
	}
	if mmr <= 6000 {
		return "Master"
	}
	return "Predator"
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

	// Start the Matchmaking Engine in the background
	go runMatchmakingEngine(ctx, collection, redisClient)

	// Worker Loop
	for {
		// BRPOP
		result, err := redisClient.BRPop(ctx, 0, "player_queue").Result()
		if err != nil {
			log.Println("Error reading from Redis:", err)
			continue
		}

		rawJSON := result[1]

		// Parse the JSON into Go struct
		var msg ClientMessage
		if err := json.Unmarshal([]byte(rawJSON), &msg); err != nil {
			log.Println("Invalid JSON received:", err)
			continue
		}

		fmt.Printf("Processing %s for %s...\n", msg.Action, msg.PlayerID)

		// Calculate Rank Tier based on incoming MMR
		rankTier := getRank(msg.SkillRating)

		filter := bson.M{"player_id": msg.PlayerID}

		update := bson.M{
			"$set": bson.M{
				"status":       "in_queue",
				"skill_rating": msg.SkillRating,
				"rank":         rankTier,
				"created_at":   time.Now(),
			},
		}

		opts := options.Update().SetUpsert(true)

		_, err = collection.UpdateOne(ctx, filter, update, opts)
		if err != nil {
			log.Println("Failed to save to MongoDB:", err)
		} else {
			fmt.Printf("Successfully upserted %s in Atlas! (MMR: %d)\n", msg.PlayerID, msg.SkillRating)

			// Broadcast with Rank and MMR included
			broadcastMsg := fmt.Sprintf(`{"player_id": "%s", "message": "Joined queue [%s | MMR: %d]"}`, msg.PlayerID, rankTier, msg.SkillRating)

			err = redisClient.Publish(ctx, "match_events", broadcastMsg).Err()
			if err != nil {
				log.Println("Failed to broadcast to Redis:", err)
			}
		}
	}
}

// matchmaking algorithm
func runMatchmakingEngine(ctx context.Context, collection *mongo.Collection, redisClient *redis.Client) {
	ticker := time.NewTicker(3 * time.Second) // Wake up every 3 seconds

	for range ticker.C {
		// distributed lock ensures multiple Go workers dont grab the same players
		lock, err := redisClient.SetNX(ctx, "matchmaking_lock", "locked", 2*time.Second).Result()
		if err != nil || !lock {
			continue // Another worker is currently doing the math, skip this tick
		}

		// Fetch players waiting in queue, sorted by skill (lowest to highest)
		findOptions := options.Find()
		findOptions.SetSort(bson.D{{Key: "skill_rating", Value: 1}})

		cursor, err := collection.Find(ctx, bson.M{"status": "in_queue"}, findOptions)
		if err != nil {
			continue
		}

		var players []PlayerSession
		if err = cursor.All(ctx, &players); err != nil {
			continue
		}

		// Group players into pairs if their skill is close
		for i := 0; i < len(players)-1; i++ {
			p1 := players[i]
			p2 := players[i+1]

			// Calculate skill difference
			diff := p1.SkillRating - p2.SkillRating
			if diff < 0 {
				diff = -diff
			}

			// If MMR difference is 200 or less = a fair match
			if diff <= 200 {
				matchID := fmt.Sprintf("match_%d", time.Now().UnixNano())

				// Update Mongo: Take both players out of the queue
				_, _ = collection.UpdateMany(ctx,
					bson.M{"player_id": bson.M{"$in": []string{p1.PlayerID, p2.PlayerID}}},
					bson.M{"$set": bson.M{"status": "in_match", "match_id": matchID}},
				)

				r1 := getRank(p1.SkillRating)
				r2 := getRank(p2.SkillRating)

				// Broadcast Match to Player 1
				msg1 := fmt.Sprintf(`{"player_id": "%s", "message": "[MATCH FOUND] You (%s) vs %s (%s)!"}`, p1.PlayerID, r1, p2.PlayerID, r2)
				redisClient.Publish(ctx, "match_events", msg1)

				// Broadcast Match to Player 2
				msg2 := fmt.Sprintf(`{"player_id": "%s", "message": "[MATCH FOUND] You (%s) vs %s (%s)!"}`, p2.PlayerID, r2, p1.PlayerID, r1)
				redisClient.Publish(ctx, "match_events", msg2)

				fmt.Printf("[MATCH FOUND] %s vs %s\n", p1.PlayerID, p2.PlayerID)

				// Skip the next player in the loop because they were just matched
				i++
			}
		}
	}
}
