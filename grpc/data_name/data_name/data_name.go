package data_name

import (
	//"fmt"
	"io"
	"math/rand"
	"time"
	"os"
	"log"
	"bufio"
	"strconv"
	"strings"
	"sync"
	//"reflect"
)


type Server struct {
	Probability float64
	BookNum *int
	Messages *int
}
//copié el mismo código de requestorder en data_data
func (server *Server) RequestOrder(stream DataName_RequestOrderServer) error {
	rand.Seed(time.Now().UnixNano())
	//fmt.println("receiving order request")
	var fileName string
	_=fileName
	for {
		ordReq, err := stream.Recv()
		if err == io.EOF {
			//fmt.print("sending response (requiest order): ")
			//acá se simula la aceptación o rechazo del orden, según una "probabilidad de exito"
			r := rand.Float64()
			if r < server.Probability {
				//fmt.println("sipo approbing ")
				*(server.Messages)=*(server.Messages)+1
				return stream.SendAndClose(&OrderRes{
					ResCode: OrderResCode_Yes,
				})
			} else {
				//fmt.println("rejecting")
				*(server.Messages)=*(server.Messages)+1
				return stream.SendAndClose(&OrderRes{
					ResCode: OrderResCode_No,
				})
			}
			//server.printTotalChunks()

		}
		if err != nil {
			return nil
		}
		//fmt.Printf("type: %T\n",upreq.Data)
		switch ordReq.Req.(type) {
		case *OrderReq_OrderData:
			//fmt.print("received request: ")
			//fmt.print(ordReq.Req.(*OrderReq_OrderData).OrderData.ChunkId)
			//fmt.print(" in node: ")
			//fmt.println(ordReq.Req.(*OrderReq_OrderData).OrderData.NodeId)
		case *OrderReq_FileName:
			fileName = ordReq.Req.(*OrderReq_FileName).FileName
			//fmt.println("receiving file of name: " + fileName)
		}
	}

	return nil
}

var mutex = &sync.Mutex{}

//similar a lo RequestOrder, pero ahora recibe los ordenes y los anota en el log
func (Server *Server) InformOrder(stream DataName_InformOrderServer) error {
	//fmt.Println("Inform Order")
	file, err := os.OpenFile("namenode/log.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)  
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	rand.Seed(time.Now().UnixNano())
	//fmt.Println("Receiving Order Request (Inform Order)")
	var fileName string
	parte := 0
	*(Server.BookNum)=*(Server.BookNum)+1
	mutex.Lock()//esto solo sería útil en el caso de que el algoritmo sea centralizado, pues si es distribuído es imposible que dos dns anoten en el log gracias al algoritmo de rikart/agrawala
	for {
		//Recibe archivo desde chunk transfer filename o chunk_id+node_id
		ordReq, err := stream.Recv()

		//Se acabo el stream
		if err == io.EOF {
			w.Flush()
			//fmt.Print("Sending Response (inform order): \n")
			*(Server.Messages)=*(Server.Messages)+1
			return stream.SendAndClose(&OrderRes{
				ResCode: OrderResCode_Yes,
			})
		}
		if err != nil {
			return nil
		}
		//fmt.Printf("type: %T\n",upreq.Data)
		switch ordReq.Req.(type) {
		case *OrderReq_OrderData:
			/*fmt.Print("Received Request (ChunkID): ")
			fmt.Print(ordReq.Req.(*OrderReq_OrderData).OrderData.ChunkId)
			fmt.Print(" In Node: ")
			fmt.Println(ordReq.Req.(*OrderReq_OrderData).OrderData.NodeId)*/

			s := strconv.Itoa(parte)
			f, err5 := os.Open("namenode/Dnodes.txt")
			if err5 != nil{
				log.Fatal(err5)
			}
			scan := bufio.NewScanner(f)
			
			bookN:=strconv.Itoa(*(Server.BookNum))
			//fmt.Println("book id: "+bookN)
			//lee primero una vez para obtener el título y el número de partes
			for i:=0; i<3; i++{
				scan.Scan()
				//fmt.Println(scan.Text())
				muchotexto := scan.Text()
				//fmt.Println(reflect.TypeOf(muchotexto))
				separados := strings.Split(muchotexto, " ")
				//separados[0]-> id; separados[1]-> ip
				//fmt.println(separados)
				//fmt.printf("separados[0] es %s", separados[0])
				id, errata := strconv.ParseInt(separados[0], 10, 64)
				if errata!=nil{
					log.Fatal(errata)
				}
				if id == ordReq.Req.(*OrderReq_OrderData).OrderData.NodeId{
					_, err := w.WriteString("parte_"+bookN+"_"+s+" "+separados[1] + "\n")
					if err != nil {
						log.Fatal(err)
					}
					f.Close()
					break
				}
			} 
			/*
			//fmt.println("parte_1_"+s+" "+strconv.FormatInt(ordReq.Req.(*OrderReq_OrderData).OrderData.NodeId,10) + "\n")
			_, err := w.WriteString("parte_1_"+s+" "+strconv.FormatInt(ordReq.Req.(*OrderReq_OrderData).OrderData.NodeId,10) + "\n")
  			if err != nil {
    			log.Fatal(err)
			  }
			  */
		case *OrderReq_FileName:
			fileName = ordReq.Req.(*OrderReq_FileName).FileName
			//fmt.println("Receiving file of name in inform order: " + fileName)
			_, err1 := w.WriteString(fileName + "\n")
  			if err1 != nil {
    			log.Fatal(err1)
			}
		}
		parte = parte + 1
	}
	mutex.Unlock()
}
