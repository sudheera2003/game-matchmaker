package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// NoSQL doc structure
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

	uri := os.Getenv("ATLAS_URI")
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatal("Failed to connect: ", err)
	}
	defer client.Disconnect(ctx)

	// select database and collection
	collection := client.Database("matchmaker").Collection("active_sessions")

	// create a new player in memory
	newPlayer := PlayerSession{
		PlayerID:  "player_99",
		Status:    "waiting_in_queue",
		CreatedAt: time.Now(),
	}

	// insert the document into Atlas
	insertResult, err := collection.InsertOne(ctx, newPlayer)
	if err != nil {
		log.Fatal("Failed to insert document: ", err)
	}

	fmt.Printf("Success, Inserted player with Database ID: %v\n", insertResult.InsertedID)
}
