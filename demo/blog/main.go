package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/microcosm-cc/bluemonday"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
)

func renderMarkdownToHTML(input string) template.HTML {
	markdown := goldmark.New(
		goldmark.WithExtensions(extension.GFM),
	)
	var buf bytes.Buffer
	if err := markdown.Convert([]byte(input), &buf); err != nil {
		fmt.Println("Error converting markdown to HTML:", err)
		return ""
	}
	policy := bluemonday.UGCPolicy()
	safeHTML := policy.Sanitize(buf.String())
	return template.HTML(safeHTML)
}

type BlogPost struct {
	Timestamp   time.Time     `json:"_timestamp"`
	Title       string        `json:"title"`
	Text        string        `json:"text"`
	HTMLContent template.HTML // rendered HTML content
	AuthorName  string        `json:"author"`
	AuthorEmail string
	Tags        []string `json:"tags"`
}

type Blog struct {
	Articles []BlogPost

	NextPage     string
	PreviousPage string
}

type RecordResponse struct {
	RecordID  string    `json:"record_id"`
	Timestamp time.Time `json:"timestamp"`
	Record    BlogPost  `json:"record"`
}

type Response struct {
	Records []RecordResponse `json:"records"`
}

func fetchBlogPosts(ctx context.Context) (*Blog, error) {
	namespace := os.Getenv("POINDEXTER_NAMESPACE")
	if namespace == "" {
		namespace = "demoblog"
	}

	baseUrl := os.Getenv("POINDEXTER_BASE")
	if baseUrl == "" {
		return nil, errors.New("POINDEXTER_BASE not set")
	}
	url := fmt.Sprintf("%s/api/query/%s/records/", baseUrl, namespace)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(`
		{"filter": {"text": null}, "limit": 10, "order_by": "-time"}
	`)))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %v", resp.StatusCode)
	}

	defer resp.Body.Close()
	var recordResponse Response

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ReadAll error: %v", err)
	}

	if err := json.Unmarshal(bodyBytes, &recordResponse); err != nil {
		return nil, err
	}

	var blog Blog
	for _, record := range recordResponse.Records {
		record.Record.Timestamp = record.Timestamp
		blog.Articles = append(blog.Articles, record.Record)
	}

	return &blog, nil
}

func blogHandler(w http.ResponseWriter, r *http.Request) {
	blog, err := fetchBlogPosts(r.Context())
	if err != nil {
		log.Printf("Failed to fetch posts: %v", err)
		http.Error(w, "Failed to fetch posts", http.StatusInternalServerError)
		return
	}

	// Parse the template file
	t, err := template.ParseFiles("template.html")
	if err != nil {
		http.Error(w, "Failed to load template", http.StatusInternalServerError)
		return
	}

	// Render Markdown to HTML and store in HTMLContent
	for i, a := range blog.Articles {
		blog.Articles[i].HTMLContent = renderMarkdownToHTML(a.Text)
	}

	// Execute the template with posts data
	err = t.Execute(w, blog)
	if err != nil {
		log.Printf("Failed to execute template: %v", err)
		http.Error(w, "Failed to execute template", http.StatusInternalServerError)
	}
}

func main() {
	if _, err := fetchBlogPosts(context.Background()); err != nil {
		log.Fatalf("Failed to fetch blog posts: %v", err)
	}
	http.HandleFunc("/", blogHandler)
	fmt.Println("Server starting on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
