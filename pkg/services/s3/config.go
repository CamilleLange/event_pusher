// Package s3 writes files to an object storage: GCS natively, or any
// S3-compatible server (MinIO locally).
package s3

import "github.com/spf13/pflag"

// ClientType selects the object storage implementation.
type ClientType string

const (
	// ClientTypeS3 talks the S3 protocol to Endpoint (MinIO, AWS...).
	ClientTypeS3 ClientType = "S3"

	// ClientTypeGCS uses the native GCS API. Only Bucket is read: credentials
	// come from the Google application default credentials.
	ClientTypeGCS ClientType = "GCS"
)

// Config is the object storage configuration.
type Config struct {
	ClientType ClientType `mapstructure:"ClientType"`
	Bucket     string     `mapstructure:"Bucket"`

	// Fields below only apply to ClientTypeS3.
	Endpoint  string `mapstructure:"Endpoint"`
	Region    string `mapstructure:"Region"`
	AccessKey string `mapstructure:"AccessKey"`
	SecretKey string `mapstructure:"SecretKey"`
	UseSSL    bool   `mapstructure:"UseSSL"`
}

// FlagSet declares the S3 flags, defaulting to defaultConfig.
func FlagSet(defaultConfig Config) *pflag.FlagSet {
	fs := pflag.NewFlagSet("s3", pflag.ContinueOnError)
	fs.String("ClientType", string(defaultConfig.ClientType), "object storage client: S3 (S3 protocol, e.g. MinIO) or GCS (native)")
	fs.String("Bucket", defaultConfig.Bucket, "bucket name")
	fs.String("Endpoint", defaultConfig.Endpoint, "S3 endpoint (host:port), S3 client only")
	fs.String("Region", defaultConfig.Region, "S3 region, S3 client only")
	fs.String("AccessKey", defaultConfig.AccessKey, "S3 access key, S3 client only")
	fs.String("SecretKey", defaultConfig.SecretKey, "S3 secret key, S3 client only")
	fs.Bool("UseSSL", defaultConfig.UseSSL, "use HTTPS to reach the S3 endpoint, S3 client only")
	return fs
}
