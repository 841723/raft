/*
 * File: 	raft.go
 * Date:    Noviembre 2023
 */
package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//

// type AplicaOperacion

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	//"crypto/rand"
	"math/rand"
	"sort"
	"sync"
	"time"

	//"net/rpc"

	"raft/internal/comun/com"
	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"

	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "read" y "write"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// funcion llamada al añadir una nueva operacion aplicada a maquina de estados
func aplica(m map[string]string, ao TipoOperacion) {
	switch ao.Operacion {
	case "read":
		// no se modifica nada
	case "write":
		m[ao.Clave] = ao.Valor
	}
}

func (nr *NodoRaft) HandleStateMachine(m map[string]string) {
	for {
		select {
		case operacion := <-nr.CanalAplicarOperacion:
			nr.Logger.Printf("%d HAS BEEN APPLIED, OPERATION \"%s\"",
				operacion.Indice, operacion.Operacion)
			aplica(m, operacion.Operacion)
			nr.Logger.Println("STATE MACHINE:", m)
		}
	}
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

type Entry struct {
	Term      int
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
//
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	CurrentTerm int
	VotedFor    int
	Log         []Entry

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	State string

	AppendEntryChannel  chan bool
	RequestVoteChannel  chan bool
	VoteReceivedChannel chan bool
	LeaderOnNodeDone    chan bool
	// LeaderTicker        chan bool

	CanalAplicarOperacion chan AplicaOperacion

	SendNewEntry [](chan bool)
}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = IntNOINICIALIZADO

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		// fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC,
				0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}

	// Codigo de inicialización
	nr.CanalAplicarOperacion = canalAplicarOperacion
	initNodoFields(nr, nodos)

	go nr.handleEstado()
	rand.Seed(time.Now().UnixNano())

	return nr
}

func initNodoFields(nr *NodoRaft, nodos []rpctimeout.HostPort) {
	nr.CurrentTerm = 1
	nr.VotedFor = IntNOINICIALIZADO

	nr.Log = append(nr.Log, Entry{})

	nr.CommitIndex = 0
	nr.LastApplied = 0
	nr.State = FOLLOWER

	nr.AppendEntryChannel = make(chan bool, 1)
	nr.RequestVoteChannel = make(chan bool, 1)
	nr.VoteReceivedChannel = make(chan bool, len(nodos))
	nr.LeaderOnNodeDone = make(chan bool, 1)
	// nr.LeaderTicker = make(chan bool, 1)
	nr.SendNewEntry = make([]chan bool, len(nodos))
	for i := range nr.SendNewEntry {
		nr.SendNewEntry[i] = make(chan bool, 10)
	}
}

// vacia todos los canales que forman parte del struct NodoRaft
func (nr *NodoRaft) clearChannels() {
	channels := make([]chan bool, 4+len(nr.SendNewEntry))
	channels = append(channels, nr.AppendEntryChannel)
	channels = append(channels, nr.RequestVoteChannel)
	channels = append(channels, nr.VoteReceivedChannel)
	channels = append(channels, nr.LeaderOnNodeDone)
	// channels = append(channels, nr.LeaderTicker)
	for i := range nr.SendNewEntry {
		channels = append(channels, nr.SendNewEntry[i])
	}
	sigo := true
	for _, ch := range channels {
		for sigo {
			select {
			case <-ch:
				// continua limpiando
			default:
				// cuando el canal ya esta vacio, se pasa al siguiente canal
				sigo = false
			}
		}
		sigo = true
	}
}

// funcion que modela la maquina de estados de cada nodo
func (nr *NodoRaft) handleEstado() {
	nr.Logger.Println("handleEstado")

	for {
		switch nr.State {

		case FOLLOWER:
			nr.handleEstadoFollower()

		case CANDIDATE:
			nr.handleEstadoCandidate()

		case LEADER:
			nr.handleEstadoLeader()
		}
	}
}

// funcion que modela el estado de SEGUIDOR
func (nr *NodoRaft) handleEstadoFollower() {
	nr.Logger.Printf("STARTS AS FOLLOWER\n")
	nr.clearChannels()

	TIME_START_ELECTION_TICKER := time.Duration(rand.Intn(151)+500) *
		time.Millisecond
	ticker := time.NewTicker(TIME_START_ELECTION_TICKER)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			//
			nr.State = CANDIDATE
			return

		case <-nr.AppendEntryChannel:
			// se ha recibido un mensaje, se reinicia el ticker
			ticker.Stop()
			ticker = time.NewTicker(TIME_START_ELECTION_TICKER)

		case <-nr.RequestVoteChannel:
			// se ha recibido un mensaje, se reinicia el ticker
			ticker.Stop()
			ticker = time.NewTicker(TIME_START_ELECTION_TICKER)
		}
	}
}

// funcion que modela el estado de CANDIDATO
func (nr *NodoRaft) handleEstadoCandidate() {
	nr.Logger.Println("STARTS AS CANDIDATE")
	nr.clearChannels()

	numberVotes := nr.startAsCandidate()
	TIME_ELECTION_TICKER := time.Duration(rand.Intn(151)+1000) * time.Millisecond
	ticker := time.NewTicker(TIME_ELECTION_TICKER)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			nr.State = CANDIDATE
			return

		case my_term_lower := <-nr.AppendEntryChannel:
			// se ha recibido un mensaje
			if my_term_lower {
				nr.State = FOLLOWER
				return
			}

		case sent := <-nr.RequestVoteChannel:
			// se ha recibido una peticion de voto
			if sent {
				nr.State = FOLLOWER
				return
			}

		case received := <-nr.VoteReceivedChannel:
			// se ha recibido un voto
			if received {
				numberVotes++
				if numberVotes*2 > len(nr.Nodos) {
					nr.State = LEADER
					return
				}
			}
		}
	}
}

func (nr *NodoRaft) startAsCandidate() int {
	nr.CurrentTerm++
	numberVotes := 1
	nr.VotedFor = nr.Yo
	nr.sendRPCsRequestVote()
	return numberVotes
}

// funcion que modela el estado de LECTOR
func (nr *NodoRaft) handleEstadoLeader() {
	nr.Logger.Println("STARTS AS LEADER")
	nr.clearChannels()

	nr.IdLider = nr.Yo
	nr.NextIndex = make([]int, len(nr.Nodos))
	nr.MatchIndex = make([]int, len(nr.Nodos))

	// crea los representantes de los seguidores e inicia los arrays
	nr.setLeadersOnNode()

	for {
		select {
		case <-nr.LeaderOnNodeDone:
			// algun representante se ha dado cuenta que ya no es lider
			nr.State = FOLLOWER
			return

		case my_term_lower := <-nr.AppendEntryChannel:
			// se ha recibido un mensaje
			if my_term_lower {
				nr.deleteLeadersOnNode()
				nr.State = FOLLOWER
				return
			}

		case sent := <-nr.RequestVoteChannel:
			// se ha recibido una peticion de voto
			if sent {
				nr.deleteLeadersOnNode()
				nr.State = FOLLOWER
				return
			}
		default:
			// se comprueba si se ha comprometido algún mensaje
			nr.updateCommitIndex()
		}
	}
}

// funcion que elimina los representantes
func (nr *NodoRaft) deleteLeadersOnNode() {
	for i := 0; i < len(nr.Nodos)-1; i++ {
		nr.LeaderOnNodeDone <- true
	}
}

// crea los representantes de los seguidores e inicia los arrays
func (nr *NodoRaft) setLeadersOnNode() {
	for i := 0; i < len(nr.Nodos); i++ {
		nr.NextIndex[i] = len(nr.Log)
		nr.MatchIndex[i] = 0
		if i != nr.Yo {
			go nr.LeaderOnNode(i)
		}
	}
}

func (nr *NodoRaft) updateCommitIndex() {
	// se ordena una copia del array MatchIndex
	sortedMatchIndex := make([]int, len(nr.MatchIndex))
	copy(sortedMatchIndex, nr.MatchIndex)
	sortedMatchIndex[nr.Yo] = len(nr.Log) - 1
	sort.Ints(sortedMatchIndex)

	// se encuentra el valor N
	N := sortedMatchIndex[(len(sortedMatchIndex)-1)/2]

	if N > nr.CommitIndex && nr.Log[N].Term == nr.CurrentTerm {
		nr.Mux.Lock()
		nr.CommitIndex = N
		nr.Mux.Unlock()
	}
}

func (nr *NodoRaft) LeaderOnNode(nodo int) {
	// se crean los structs a mandar

	ticker := time.NewTicker(60 * time.Millisecond)
	for {
		select {
		case <-nr.LeaderOnNodeDone:
			// algun representante se ha dado cuenta que ya no es lider
			return

		case <-nr.SendNewEntry[nodo]:
			// el lider ha recibido una peticion del cliente
			nr.Logger.Printf("NUEVA REQUEST representante de nodo %d\n", nodo)
			ticker.Stop()
			if exit := nr.handleAppendEntries(nodo); exit {
				return
			}
			ticker = time.NewTicker(60 * time.Millisecond)

		case <-ticker.C:
			// se debe mandar el latido periodico
			nr.Logger.Printf("TICKER representante de nodo %d\n", nodo)
			ticker.Stop()
			if exit := nr.handleAppendEntries(nodo); exit {
				return
			}
			ticker = time.NewTicker(60 * time.Millisecond)
		}
	}
}

// funcion que envia AppendEntry y gestiona la recepcion, enviando mas mensajes
// necesarios para poner al dia a seguidor, o se da cuenta que ya no es lider
// devuelve 'true' cuando ya no es lider
func (nr *NodoRaft) handleAppendEntries(nodo int) (exit bool) {

	nr.Mux.Lock()
	args := ArgAppendEntries{
		Term:         nr.CurrentTerm,
		LeaderId:     nr.Yo,
		PrevLogIndex: nr.NextIndex[nodo] - 1,
		PrevLogTerm:  nr.Log[nr.NextIndex[nodo]-1].Term,
		LeaderCommit: nr.CommitIndex}
	nr.Mux.Unlock()
	results := Results{Success: false}

	// se envia primer mensaje
	var err error
	if err, exit = nr.sendAppendEntries_RPC(nodo, args, &results); exit {
		return true
	}
	for err == nil && !results.Success {
		// itera hasta conseguir que seguidor este al dia enviando mas mensajes
		nr.NextIndex[nodo]--
		nr.Mux.Lock()
		args.PrevLogIndex--
		args.PrevLogTerm = nr.Log[args.PrevLogIndex].Term
		args.Entries = nr.Log[nr.NextIndex[nodo]:]
		nr.Mux.Unlock()
		// se envia el siguiente mensaje
		if err, exit = nr.sendAppendEntries_RPC(nodo, args, &results); exit {
			return true
		}
	}
	if results.Success {
		// actualiza los valores cuando seguidor este al dia
		nr.updateIndex(nodo)
	}
	return false
}

// funcion que hace la llamada RPC
// devuelve en err el valor devuelto por CallTimeout
// devuelve en exit 'true' si nodo ya no es lider, en ese caso avisa a otros
// representantes a traves de canal LeaderOnNodeDone
func (nr *NodoRaft) sendAppendEntries_RPC(nodo int, args ArgAppendEntries,
	results *Results) (err error, exit bool) {

	// args.LeaderCommit = nr.CommitIndex
	err = nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries_RPC", &args,
		results, 1001*time.Millisecond)
	if nr.CurrentTerm < results.Term {
		// se comprueba que nodo ya no es lider
		for i := 0; i < len(nr.Nodos)-1; i++ {
			nr.LeaderOnNodeDone <- true
		}
		return err, true
	}
	return err, false
}

func (nr *NodoRaft) updateIndex(nodo int) {
	nr.Mux.Lock()
	nr.NextIndex[nodo] = len(nr.Log)
	nr.MatchIndex[nodo] = nr.NextIndex[nodo] - 1
	nr.Mux.Unlock()
}

// funcion que solicita votos para ser lider a los demas servidores,
// se introduce el valor devuelto en el canal VoteReceivedChannel
func (nr *NodoRaft) sendRPCsRequestVote() {
	// se crean los structs a enviar
	args := ArgsPeticionVoto{Term: nr.CurrentTerm,
		CandidateID:  nr.Yo,
		LastLogIndex: len(nr.Log) - 1,
		LastLogTerm:  nr.Log[len(nr.Log)-1].Term}
	var reply RespuestaPeticionVoto

	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			go func(i int) {
				// se envian de manera asincrona todas las peticiones
				if nr.enviarPeticionVoto(i, &args, &reply) {
					nr.VoteReceivedChannel <- reply.VoteGranted
				}
			}(i)
		}
	}
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int, int, []int) {
	return nr.Yo, nr.CurrentTerm, nr.IdLider == nr.Yo,
		nr.IdLider, nr.CommitIndex, nr.MatchIndex
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en  entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	indice := -1
	mandato := -1
	EsLider := (nr.IdLider == nr.Yo)
	idLider := nr.IdLider
	valorADevolver := "NO COMPROMETIDO"

	if EsLider {
		mandato = nr.CurrentTerm
		nr.Mux.Lock()
		indice = len(nr.Log)
		nr.Log = append(nr.Log, Entry{Term: nr.CurrentTerm, Operacion: operacion})
		nr.Logger.Printf("REQUEST FROM CLIENT, INDEX %d\n", indice)
		nr.Mux.Unlock()

		for idx := 0; idx < len(nr.Nodos); idx++ {
			// se comunica a todos los representantes que hay  nueva request
			if idx != nr.Yo {
				nr.SendNewEntry[idx] <- true
			}
		}
		// se espera a consolidar la request pedida por el cliente
		for indice > nr.CommitIndex {
		}
		valorADevolver = "COMPROMETIDO CON EXITO"
	}
	return indice, mandato, EsLider, idLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	nr.Logger.Printf("Se ha parado el nodo %d\n", nr.Yo)
	return nil
}

type EstadoParcial struct {
	Mandato     int
	EsLider     bool
	IdLider     int
	CommitIndex int
	MatchIndex  []int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider, reply.CommitIndex,
		reply.MatchIndex = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.

type ArgsPeticionVoto struct {
	// Vuestros datos aqui
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// Structura de ejemplo de respuesta de RPC PedirVoto,

type RespuestaPeticionVoto struct {
	// Vuestros datos aqui
	Term        int
	VoteGranted bool
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto_RPC(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {

	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	nr.Logger.Printf("EJECUTANDO PEDIRVOTO_RPC, PEDIDO POR %d",
		peticion.CandidateID)

	if peticion.Term > nr.CurrentTerm {
		// se actualiza al nuevo mandato
		nr.CurrentTerm = peticion.Term
		nr.VotedFor = IntNOINICIALIZADO
		nr.IdLider = IntNOINICIALIZADO
	}

	reply.Term = nr.CurrentTerm
	if peticion.Term < nr.CurrentTerm {
		// candidato no esta al dia
		nr.Logger.Printf("NO SE ENVIA VOTO A %d EN MANDATO %d",
			peticion.CandidateID, nr.CurrentTerm)
		reply.VoteGranted = false

	} else if nr.VotedFor == IntNOINICIALIZADO ||
		nr.VotedFor == peticion.CandidateID || nr.isCandidateUpToDate(peticion) {
		// no se ha votado a nadie en este mandato
		nr.Logger.Printf("SI SE ENVIA VOTO A %d EN MANDATO %d",
			peticion.CandidateID, nr.CurrentTerm)
		reply.VoteGranted = true
		nr.VotedFor = peticion.CandidateID
	} else {
		// ya se ha votado en este mandato
		nr.Logger.Printf("NO SE ENVIA VOTO A %d EN MANDATO %d",
			peticion.CandidateID, nr.CurrentTerm)
		reply.VoteGranted = false
	}

	nr.RequestVoteChannel <- reply.VoteGranted
	return nil
}

func (nr *NodoRaft) isCandidateUpToDate(args *ArgsPeticionVoto) bool {
	return args.LastLogTerm > nr.Log[len(nr.Log)-1].Term ||
		(args.LastLogTerm == nr.Log[len(nr.Log)-1].Term &&
			args.LastLogIndex >= len(nr.Log)-1)
}

type ArgAppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type Results struct {
	Term    int
	Success bool
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries_RPC(args *ArgAppendEntries,
	results *Results) error {
	nr.Logger.Printf("EJECUTANDO APPENDENTRIES_RPC, PEDIDO POR %d",
		args.LeaderId)

	nr.decideIfAcceptAppendEntries(args)

	if len(args.Entries) == 0 {
		nr.Logger.Printf("HEARTBEAT SIN ENTRIES, PEDIDO POR %d", args.LeaderId)
	} else {
		nr.Logger.Printf("APPENDENTRIES CON %d ENTRIES, PEDIDO POR %d",
			len(args.Entries), args.LeaderId)
	}

	results.Term = nr.CurrentTerm
	nr.Logger.Println("args:", args)
	if args.Term < nr.CurrentTerm || args.PrevLogIndex > len(nr.Log)-1 ||
		nr.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// si yo no estoy actualizado, pido entries anteriores
		results.Success = false
		return nil
	}
	nr.updateLog(args)
	// se han consolidado algunas entradas, se aplican (maquina de estados)
	nr.applyEntries()

	if args.LeaderCommit > nr.CommitIndex {
		nr.Mux.Lock()
		nr.CommitIndex = com.Min(args.LeaderCommit, len(nr.Log)-1)
		nr.Mux.Unlock()
	}
	results.Success = true
	return nil
}

// funcion que aplica las entradas del log en la maquina de estados
func (nr *NodoRaft) applyEntries() {
	nr.Logger.Printf("entra en applyEntries")

	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	for nr.CommitIndex > nr.LastApplied {
		nr.LastApplied++
		// se aplica la entrada en la maquina de estados
		ao := AplicaOperacion{Indice: nr.LastApplied, Operacion: nr.Log[nr.LastApplied].Operacion}
		nr.CanalAplicarOperacion <- ao
	}
}

func (nr *NodoRaft) decideIfAcceptAppendEntries(args *ArgAppendEntries) {
	if args.Term >= nr.CurrentTerm {
		nr.Mux.Lock()
		nr.CurrentTerm = args.Term
		nr.IdLider = args.LeaderId
		nr.Mux.Unlock()
		nr.AppendEntryChannel <- true
	} else {
		// el LeaderId del append entries no esta actualizado
		nr.AppendEntryChannel <- false
	}
}

// funcion que actualiza el vector de log del servidor con los recibidos por el
// lider
func (nr *NodoRaft) updateLog(args *ArgAppendEntries) {
	i := args.PrevLogIndex + 1
	for _, entry := range args.Entries {
		if i < len(nr.Log)-1 && nr.Log[i].Term == entry.Term {
			i++
		} else {
			nr.Log = nr.Log[:i]
			nr.Log = append(nr.Log, entry)
			nr.Logger.Println("LOG:", nr.Log)
			i++
		}
	}
}

// ----- Metodos/Funciones a utilizar como clientes
//
//

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto_RPC", args, reply,
		1001*time.Millisecond)
	nr.Logger.Printf("PIDIENDO VOTO A %d\n", nodo)
	if err != nil {
		nr.Logger.Printf("error en timeout pidiendo voto desde %d al %d\n",
			nr.Yo, nodo)
		return false
	}

	if reply.VoteGranted {
		nr.Logger.Printf("SI SE HA RECIBIDO UN VOTO DE %d\n", nodo)
	} else {
		nr.Logger.Printf("NO SE HA RECIBIDO UN VOTO DE %d\n", nodo)
	}
	return true
}
