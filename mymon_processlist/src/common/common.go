package common

import (
	"github.com/go-ini/ini"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native" // Native engine
)

const (
	GConfig          = "./mymon.ini"
	Default_key      = "mysql"
	Default_user     = "root"
	Default_password = "root"
	Default_host     = "127.0.0.1"
	Default_port     = "root"
)
const (
	section_key  = "mysql"
	section_user = "user"
	section_host = "host"
	section_pwd  = "password"
	section_port = "port"
)

var g_db mysql.Conn

func Init_database() (mysql.Conn, error) {
	if g_db != nil {
		return g_db, nil
	}
	cfg, err := ini.InsensitiveLoad(GConfig)
	if err != nil {
		return nil, err
	}
	sec, err := cfg.GetSection(section_key)
	if err != nil {
		return nil, err
	}
	user := sec.Key(section_user).String()
	host := sec.Key(section_host).String()
	pwd := sec.Key(section_pwd).String()
	if len(user) == 0 {
		user = Default_user
	}
	if len(host) == 0 {
		host = Default_host
	}
	if len(pwd) == 0 {
		pwd = Default_password
	}
	port := sec.Key(section_port).String()
	if err != nil {
		port = Default_port
	}
	db := mysql.New("tcp", "", host+":"+port, user, pwd, "information_schema")

	err = db.Connect()
	if err != nil {
		return nil, err
	}
	g_db = db
	return g_db, nil
}
