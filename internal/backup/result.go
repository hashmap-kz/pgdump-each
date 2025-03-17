package backup

type ResultInfo struct {
	Host   string
	Port   int
	Dbname string
	Err    error
}
