package app

type Application struct {
	server *server.Server
	conf   *Config
}

func (a *Application) Run() error {
	return a.server.Serve(a.conf.Address())
}

func (a *Application) Stop() {
	a.server.Stop()
}
