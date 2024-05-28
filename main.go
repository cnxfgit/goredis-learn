package main

func main() {
	// 构造server实例
	server, err := app.ConstructServer()
	if err != nil {
		panic(err)
	}

	// 构造application实例
	app:= app.NewApplication(server, app.SetUpConfig())
	defer app.Stop()

	// 运行application
	if err := app.Run(); err != nil {
		panic(err)
	}	
}