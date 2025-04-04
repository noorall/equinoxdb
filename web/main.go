package main

import (
	"context"
	"embed"
	"equinox/web/common"
	"equinox/web/executor"
	"flag"
	"fmt"
	"github.com/gin-contrib/sessions/cookie"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

var db *gorm.DB

//go:embed templates/*.html
var tmplFS embed.FS

func loadTemplates() *template.Template {
	tmpl := template.Must(template.New("").Funcs(template.FuncMap{
		"formatTime": formatTime,
		"div": func(a, b int) float64 {
			if b == 0 {
				return 0
			}
			return float64(a) / float64(b)
		},
		"byteToMB": func(a int64) int64 {
			return a / 1024 / 1024
		},
		"fileName": func(a string) string {
			return filepath.Base(a)
		},
		"unixMilli": func(t time.Time) int64 {
			return t.UnixMilli() // 毫秒级时间戳（Go 1.17+）
		},
	}).ParseFS(tmplFS, "templates/*.html"))
	return tmpl
}

var username, password string
var cancels sync.Map

func main() {
	var port string
	var logging bool
	flag.StringVar(&port, "port", "9898", "指定服务监听的端口")
	flag.StringVar(&username, "username", "admin", "指定登录用户名")
	flag.StringVar(&password, "password", "password", "指定登录密码")
	flag.BoolVar(&logging, "logging", false, "是否启用请求日志，默认为 true")
	flag.Parse()

	fmt.Println("WebUI配置：")
	fmt.Printf("端口: %s\n", port)
	fmt.Printf("用户名: %s\n", username)
	fmt.Printf("密码: %s\n", password) // 注意，密码是敏感信息，这里可能会根据需求决定是否打印
	fmt.Printf("启用日志: %v\n", logging)

	if !logging {
		gin.DefaultWriter = io.Discard
	}
	var err error
	db, err = gorm.Open(sqlite.Open("tasks.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	err = db.AutoMigrate(&common.Task{}, &common.FieldTypeEntry{})
	if err != nil {
		panic("failed to migrate database")
	}

	err = initTestDb()
	if err != nil {
		panic("failed to init test database")
	}

	r := gin.Default()

	var tasks []common.Task
	db.Find(&tasks)

	r.SetFuncMap(template.FuncMap{
		"formatTime": formatTime,
	})

	store := cookie.NewStore([]byte("secret"))
	store.Options(sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode, // 临时用于本地非 HTTPS
		Secure:   false,                // 仅本地测试时使用
	})
	r.Use(sessions.Sessions("mysession", store))

	r.SetHTMLTemplate(loadTemplates())
	r.GET("/login", loginPage)
	r.POST("/login", login)
	r.POST("/exec", exec)

	protected := r.Group("/")
	protected.Use(checkLogin)
	{
		protected.GET("/", showTaskList)
		protected.GET("/task/new", showTaskAdd)
		protected.POST("/task/save", saveTask)
		protected.POST("/task/stop/:id", stopTask)
		protected.POST("/task/delete/:id", deleteTask)
		protected.GET("/task/status/:id", getTaskStatus)
		protected.GET("/task/compare", compareTasks)
	}
	resetAllTasks()
	err = r.Run("0.0.0.0:" + port)
	if err != nil {
		return
	}
}

func resetAllTasks() {
	db.Model(&common.Task{}).Where("finished = ?", false).Update("stopped", true)
}

func showTaskList(c *gin.Context) {
	var tasks []common.Task
	db.Order("id DESC").Find(&tasks)
	c.HTML(http.StatusOK, "index.html", gin.H{"tasks": tasks})
}

func showTaskAdd(c *gin.Context) {
	c.HTML(http.StatusOK, "task_add.html", gin.H{
		"Action": "/task/save",
		"Task":   nil,
	})
}

func saveTask(c *gin.Context) {
	var task common.Task
	if id := c.PostForm("id"); id != "" {
		db.First(&task, id)
	}
	task.DataPath = c.PostForm("data_path")

	info, err := os.Stat(task.DataPath)
	if err != nil {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, `
			<script>
				alert("数据集不存在！");
				history.back();
			</script>
		`)
		return
	}

	task.DataSize = info.Size()
	task.CreatedAt = time.Now()
	task.Thread, _ = strconv.Atoi(c.PostForm("thread"))
	task.Type, _ = strconv.Atoi(c.PostForm("type"))
	db.Save(&task)
	ctx, cancel := context.WithCancel(context.Background())
	cancels.Store(task.ID, cancel)
	go executor.RunMultiWriteTask(ctx, &task, db)
	c.Redirect(http.StatusSeeOther, "/")
}

func deleteTask(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return
	}
	if cancel, ok := cancels.Load(uint(id)); ok {
		if cancelFunc, ok := cancel.(context.CancelFunc); ok {
			var task common.Task
			db.First(&task, id)
			task.Stopped = true
			db.Save(&task)
			cancelFunc()
		}
	}
	db.Delete(&common.Task{}, id)
	c.Redirect(http.StatusSeeOther, "/")
}

func stopTask(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return
	}
	if cancel, ok := cancels.Load(uint(id)); ok {
		if cancelFunc, ok := cancel.(context.CancelFunc); ok {
			var task common.Task
			db.First(&task, id)
			task.Stopped = true
			db.Save(&task)
			cancelFunc()
		} else {
			log.Println("cancel function type assertion failed")
		}
	}
	c.Redirect(http.StatusSeeOther, "/")
}

func getTaskStatus(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return
	}
	var task common.Task
	err = db.First(&task, id).Error

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"is_finished":     task.Finished,
		"created_at":      formatTime(task.CreatedAt),
		"finished_at":     formatTime(task.FinishedAt),
		"status_progress": task.Progress,
		"is_stopped":      task.Stopped,
	})
}

func loginPage(c *gin.Context) {
	c.HTML(http.StatusOK, "login.html", nil)
}

func login(c *gin.Context) {
	user := c.PostForm("username")
	pass := c.PostForm("password")

	if user == username && pass == password {
		session := sessions.Default(c)
		session.Set("logged_in", true)
		session.Save()

		c.Redirect(http.StatusFound, "/")
	} else {
		c.HTML(http.StatusUnauthorized, "login.html", gin.H{
			"message": "用户名或密码错误",
		})
	}
}

func checkLogin(c *gin.Context) {
	session := sessions.Default(c)
	loggedIn, ok := session.Get("logged_in").(bool)

	if !ok || !loggedIn {
		c.Redirect(http.StatusFound, "/login")
		c.Abort()
		return
	}

	c.Next()
}
