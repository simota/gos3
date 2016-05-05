package gos3

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/fasthttp"
	"github.com/labstack/echo/engine/standard"
)

const (
	BucketPath = "./buckets"
	HeaderEtag = "Etag"
)

func getKey(c echo.Context) error {
	bucket := &Bucket{Path: BucketPath, Name: c.Param("bucket")}
	key := bucket.Lookup(c.Param("key"))
	if !key.IsStored() {
		return c.String(http.StatusNotFound, "404 Not Found")
	}

	f, err := key.Content()
	if err != nil {
		return c.String(http.StatusInternalServerError, "500 Internal Server Error")
	}
	defer f.Close()
	fi, _ := f.Stat()

	res := c.Response()

	modtime := fi.ModTime()
	meta := key.Metadata()

	res.Header().Set(echo.HeaderContentType, meta.ContentType)
	res.Header().Set(echo.HeaderLastModified, modtime.UTC().Format(http.TimeFormat))
	res.WriteHeader(http.StatusOK)
	_, err = io.Copy(res, f)
	return err
}

func headKey(c echo.Context) error {
	bucket := &Bucket{Path: BucketPath, Name: c.Param("bucket")}
	key := bucket.Lookup(c.Param("key"))
	if !key.IsStored() {
		return c.String(http.StatusNotFound, "404 Not Found")
	}
	meta := key.Metadata()
	c.Response().Header().Set(echo.HeaderContentLength, strconv.Itoa(meta.ContentLength))
	return c.String(http.StatusOK, "")
}

func saveKey(c echo.Context) error {
	req := c.Request()
	bucket := &Bucket{Path: BucketPath, Name: c.Param("bucket")}
	key := bucket.NewKey()
	key.Name = c.Param("key")
	digest, err := key.Save(req.Body(), req.Header().Get(echo.HeaderContentType))
	if err != nil {
		return c.String(http.StatusBadRequest, "")
	}
	c.Response().Header().Set(HeaderEtag, fmt.Sprintf("\"%s\"", digest))

	return c.String(http.StatusOK, "")
}

func deleteKey(c echo.Context) error {
	bucket := &Bucket{Path: BucketPath, Name: c.Param("bucket")}
	key := bucket.Lookup(c.Param("key"))
	if err := key.Delete(); err != nil {
		return c.String(http.StatusNotFound, "404 Not Found")
	}
	return c.String(http.StatusNoContent, "")
}

func saveBucket(c echo.Context) error {
	bucket := &Bucket{Path: BucketPath, Name: c.Param("bucket")}
	bucket.Create()
	return c.String(http.StatusOK, "")
}

func commonHeader(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		log.Println(c.Request().Method(), c.Request().URI())
		c.Response().Header().Set(echo.HeaderAccessControlAllowOrigin, "*")
		c.Response().Header().Set(echo.HeaderContentType, "text/xml")
		return next(c)
	}
}

func NewServer() *echo.Echo {
	e := echo.New()
	e.Use(commonHeader)
	e.PUT("/:bucket/", saveBucket)
	e.PUT("/:bucket/:key", saveKey)
	e.GET("/:bucket/:key", getKey)
	e.HEAD("/:bucket/:key", headKey)
	e.DELETE("/:bucket/:key", deleteKey)
	return e
}

func Run(port string, fast bool, debug bool) {
	e := NewServer()
	e.SetDebug(debug)
	if fast {
		log.Println("run fast http server")
		e.Run(fasthttp.New(port))
	} else {
		log.Println("run standard http server")
		e.Run(standard.New(port))
	}
}
