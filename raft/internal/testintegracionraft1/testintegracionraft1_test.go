/*
 * File: 	raft.go
 * Date:    Noviembre 2023
 */

package testintegracionraft1

import (
	"fmt"
	"os"
	"path/filepath"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"

	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	// hosts
	MAQUINA1_CLUSTER = "192.168.3.1"
	MAQUINA2_CLUSTER = "192.168.3.2"
	MAQUINA3_CLUSTER = "192.168.3.3"

	//puertos
	PUERTOREPLICA1_CLUSTER = "29251"
	PUERTOREPLICA2_CLUSTER = "29252"
	PUERTOREPLICA3_CLUSTER = "29253"

	//nodos replicas
	REPLICA1 = MAQUINA1_CLUSTER + ":" + PUERTOREPLICA1_CLUSTER
	REPLICA2 = MAQUINA2_CLUSTER + ":" + PUERTOREPLICA2_CLUSTER
	REPLICA3 = MAQUINA3_CLUSTER + ":" + PUERTOREPLICA3_CLUSTER

	// REPLICA1 = MAQUINA1_LOCAL + ":" + PUERTOREPLICA1_LOCAL
	// REPLICA2 = MAQUINA2_LOCAL + ":" + PUERTOREPLICA2_LOCAL
	// REPLICA3 = MAQUINA3_LOCAL + ":" + PUERTOREPLICA3_LOCAL

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_ed25519"

	timeout_time = 4000 // ms
)

// PATH de los ejecutables de modulo golang de servicio Raft
// var PATH string = filepath.Join(os.Getenv("HOME"), "tmp", "p4", "raft")

var PATH string = filepath.Join("/", "misc", "alumnos", "sd", "sd2324",
	"a841723", "practica4", "raft")

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; go run " + EXECREPLICA

// // TEST primer rango
// func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
// 	// <setup code>
// 	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
// 	cfg := makeCfgDespliegue(t,
// 		3,
// 		[]string{REPLICA1, REPLICA2, REPLICA3},
// 		[]bool{true, true, true})

// 	// tear down code
// 	// eliminar procesos en máquinas remotas
// 	defer cfg.stop()

// 	// Run test sequence

// 	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
// 	t.Run("T1:soloArranqueYparada",
// 		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

// 	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
// 	t.Run("T2:ElegirPrimerLider",
// 		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

// 	// Test3: tenemos el primer primario correcto
// 	t.Run("T3:FalloAnteriorElegirNuevoLider",
// 		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

// 	// Test4: Tres operaciones comprometidas en configuración estable
// 	t.Run("T4:tresOperacionesComprometidasEstable",
// 		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })
// }

// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T6:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T7:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ?? - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	// t.Skip("TEST1 - SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// // Comprobar estado replica 0
	// cfg.comprobarEstadoRemoto(0, 0, false, -1)

	// // Comprobar estado replica 1
	// cfg.comprobarEstadoRemoto(1, 0, false, -1)

	// // Comprobar estado replica 2
	// cfg.comprobarEstadoRemoto(2, 0, false, -1)

	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	// t.Skip("TEST2 - SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	// t.Skip("TEST3 - SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)
	time.Sleep(2000 * time.Millisecond)

	// Desconectar lider
	var reply raft.Vacio
	_, _, _, idLider := cfg.obtenerEstadoRemoto(1)
	cfg.conectados[idLider] = false
	fmt.Printf("el lider es %d\n", idLider)
	err := cfg.nodosRaft[idLider].CallTimeout("NodoRaft.ParaNodo", raft.Vacio{},
		&reply, 4001*time.Millisecond)
	check.CheckError(err, "Error en llamada a para nodo")

	time.Sleep(500 * time.Millisecond)
	fmt.Printf("Comprobar nuevo lider\n")
	cfg.pruebaUnLider(3)
	fmt.Printf("lider comprobado correctamente\n")

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	// t.Skip("TEST4 - SKIPPED tresOperacionesComprometidasEstable")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	_, _, _, idLider := cfg.obtenerEstadoRemoto(0)
	fmt.Printf("el lider es %d\n", idLider)

	tipo_operacion := raft.TipoOperacion{Operacion: "leer", Clave: "", Valor: ""}
	var reply raft.ResultadoRemoto

	for i := 0; i < 3; i++ {
		time.Sleep(1500 * time.Millisecond)
		fmt.Printf("enviando request de lectura al lider %d\n", idLider)

		err := cfg.nodosRaft[idLider].CallTimeout(
			"NodoRaft.SometerOperacionRaft", tipo_operacion, &reply,
			4001*time.Millisecond)
		check.CheckError(err, "Error en llamada a SometerOperacionRaft")
		fmt.Printf(
			"MENSAJE indice %d en mandato %d, (lider %d) valor devuelto:%s\n",
			reply.IndiceRegistro, reply.EstadoParcial.Mandato,
			reply.EstadoParcial.IdLider, reply.ValorADevolver)
	}

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	// t.Skip("TEST5 - SKIPPED AcuerdoApesarDeSeguidor")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	//  Obtener un lider y, a continuación desconectar una de los nodos Raft
	cfg.t.Log("se busca que haya solo 1 lider")
	cfg.pruebaUnLider(3)
	
	_, _, _, idLider := cfg.obtenerEstadoRemoto(0)
	cfg.t.Log("el lider es ", idLider)

	cfg.sendNewEntry(3, 2000, idLider)

	// desconectar una replica
	idReplica := (idLider + 1) % 3
	cfg.t.Log("parando seguidor ", idReplica)
	cfg.stopDistributedNode(idReplica)

	// Comprobar varios acuerdos con una réplica desconectada
	cfg.t.Log("se comprometen entradas solo con 2 replicas")
	cfg.sendNewEntry(3, 2000, idLider)

	// reconectar nodo Raft previamente desconectado y comprobar varios acuerdos
	cfg.startDistributedNode(idReplica)
	cfg.t.Log("se comprometen entradas con 3 replicas de nuevo")
	cfg.sendNewEntry(3, 2000, idLider)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	// t.Skip("TEST6 - SKIPPED SinAcuerdoPorFallos")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	_, _, _, idLider := cfg.obtenerEstadoRemoto(0)
	cfg.t.Log("el lider es ", idLider)

	// Comprometer una entrada
	cfg.sendNewEntry(1, 1500, idLider)

	//  Obtener un lider y, a continuación desconectar 2 de los nodos Raft
	idReplica1 := (idLider + 1) % 3
	idReplica2 := (idLider + 2) % 3
	cfg.t.Log("parando seguidor ", idReplica1)
	cfg.stopDistributedNode(idReplica1)

	cfg.t.Log("parando seguidor ", idReplica2)
	cfg.stopDistributedNode(idReplica2)

	time.Sleep(1500 * time.Millisecond)

	// Comprobar varios acuerdos con 2 réplicas desconectada
	cfg.t.Log("se comprometen entradas solo con 1 replica")
	// da error porque no le responden replicas y CallTimeout de timeout
	// nose como solucionarlo
	cfg.sendNewEntry(3, 1500, idLider)

	// reconectar lo2 nodos Raft  desconectados y probar varios acuerdos
	cfg.startDistributedNode(idReplica1)
	cfg.startDistributedNode(idReplica2)
	cfg.t.Log("se comprometen entradas con 3 replicas de nuevo")
	cfg.sendNewEntry(3, 1500, idLider)

	time.Sleep(1500 * time.Millisecond)
	cfg.checkSameState(3)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	// t.Skip("TEST7 - SKIPPED SometerConcurrentementeOperaciones")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// un bucle para estabilizar la ejecucion
	time.Sleep(1500 * time.Millisecond)

	// Obtener un lider y, a continuación someter una operacion
	_, _, _, idLider := cfg.obtenerEstadoRemoto(0)
	cfg.sendNewEntry(3, 1500, idLider)

	// Someter 5  operaciones concurrentes
	cfg.sendNewEntryCon(10, idLider)
	// Comprobar estados de nodos Raft, sobre todo
	// el avance del mandato en curso e indice de registro de cada uno
	// que debe ser identico entre ellos
	time.Sleep(1500 * time.Millisecond)
	cfg.checkSameState(3)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	if cfg.conectados[indiceNodo] {
		err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
			raft.Vacio{}, &reply, 4001*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")
	}

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)
	cfg.t.Log("EXECREPLICACMD: ", EXECREPLICACMD)
	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)

		// dar tiempo para se establezcan las replicas
		time.Sleep(2000 * time.Millisecond)
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(2000 * time.Millisecond)
}

// inicia el proceso de replica de un nodo dado
func (cfg *configDespliegue) startDistributedNode(node int) {
	cfg.t.Log("Before starting node ", node, " EXECREPLICACMD:", EXECREPLICACMD)
	if !cfg.conectados[node] {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(node)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{string(cfg.nodosRaft[node].Host())}, cfg.cr, PRIVKEYFILE)
	}
	cfg.conectados[node] = true
	time.Sleep(500 * time.Millisecond)
}

func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for idx, endPoint := range cfg.nodosRaft {
		if cfg.conectados[idx] {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 4001*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
		}
	}
}

func (cfg *configDespliegue) stopDistributedNode(node int) {

	var reply raft.Vacio

	if cfg.conectados[node] {
		err := cfg.nodosRaft[node].CallTimeout("NodoRaft.ParaNodo",
			raft.Vacio{}, &reply, 4001*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC Para nodo")
	}
	cfg.conectados[node] = false
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}
}

func (cfg *configDespliegue) sendNewEntry(cant, delay, idLeader int) {

	tipo_operacion := raft.TipoOperacion{Operacion: "write"}
	var reply raft.ResultadoRemoto
	reply.IndiceRegistro = -1
	reply.EstadoParcial.Mandato = -1
	reply.EstadoParcial.EsLider = false
	reply.EstadoParcial.IdLider = -1
	reply.ValorADevolver = "NO COMPROMETIDO"

	for i := 0; i < cant; i++ {
		fmt.Printf("enviando request de lectura al lider %d\n", idLeader)
		tipo_operacion.Valor = strconv.Itoa(i)
		tipo_operacion.Clave = "user" + strconv.Itoa(i)

		err := cfg.nodosRaft[idLeader].CallTimeout(
			"NodoRaft.SometerOperacionRaft", tipo_operacion, &reply,
			time.Duration(timeout_time)*time.Millisecond)
		checkError(err, "Error en llamada a SometerOperacionRaft")

		fmt.Printf("\tmandato:%d  soylider?:%v,  indice:%d,  valor:%s\n",
			reply.EstadoParcial.Mandato, reply.EstadoParcial.EsLider,
			reply.IndiceRegistro, reply.ValorADevolver)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
}

func (cfg *configDespliegue) sendNewEntryCon(cant, idLeader int) {

	tipo_operacion := raft.TipoOperacion{Operacion: "write"}
	reply := make([]raft.ResultadoRemoto, cant)
	for _, rep := range reply {
		rep.IndiceRegistro = -1
		rep.EstadoParcial.Mandato = -1
		rep.EstadoParcial.EsLider = false
		rep.EstadoParcial.IdLider = -1
		rep.ValorADevolver = "NO CONSOLIDADO"
	}

	for i := 0; i < cant; i++ {
		fmt.Printf("enviando request de lectura al lider %d\n", idLeader)
		go func(node int, reply raft.ResultadoRemoto) {

			tipo_operacion.Valor = strconv.Itoa(node)
			tipo_operacion.Clave = "user" + strconv.Itoa(node)

			err := cfg.nodosRaft[idLeader].CallTimeout(
				"NodoRaft.SometerOperacionRaft", tipo_operacion, &reply,
				time.Duration(timeout_time)*time.Millisecond)
			checkError(err, "Error en llamada a SometerOperacionRaft")

			fmt.Printf("\tmandato:%d  soylider?:%v,  indice:%d,  valor:%s\n",
				reply.EstadoParcial.Mandato, reply.EstadoParcial.EsLider,
				reply.IndiceRegistro, reply.ValorADevolver)
		}(i, reply[i])
	}
}

func checkError(err error, comment string) {
	if err != nil {
		if err.Error() == "SometerOperacionRaft_timeout" {
			return
		} else {
			fmt.Fprintf(os.Stderr, "In: %s, Fatal error: %s", comment, err.Error())
			os.Exit(1)
		}
	}
}

func (cfg *configDespliegue) checkSameState(cant int) {
	if cant == 3 {
		var reply1 raft.EstadoRemoto
		var reply2 raft.EstadoRemoto
		var reply3 raft.EstadoRemoto

		err := cfg.nodosRaft[0].CallTimeout("NodoRaft.ObtenerEstadoNodo",
			raft.Vacio{}, &reply1, 4001*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

		err = cfg.nodosRaft[1].CallTimeout("NodoRaft.ObtenerEstadoNodo",
			raft.Vacio{}, &reply2, 4001*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

		err = cfg.nodosRaft[2].CallTimeout("NodoRaft.ObtenerEstadoNodo",
			raft.Vacio{}, &reply3, 4001*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

		if reply1.IdNodo == reply2.IdNodo || reply1.IdNodo == reply3.IdNodo ||
			reply1.Mandato != reply2.Mandato || reply1.Mandato != reply3.Mandato ||
			reply1.IdLider != reply2.IdLider || reply1.IdLider != reply3.IdLider ||
			reply1.CommitIndex != reply2.CommitIndex ||
			reply1.CommitIndex != reply3.CommitIndex ||
			!leaderHasRightValuesOfFollowers(reply1, reply2, reply3) {

			cfg.t.Log(reply1.CommitIndex, reply2.CommitIndex, reply3.CommitIndex)
			cfg.t.Fatalf("Estado incorrecto en replicas en subtest %s",
				cfg.t.Name())
		} else {
			cfg.t.Log("Estado correcto en replicas")
		}
	}
}

func leaderHasRightValuesOfFollowers(reply1, reply2, reply3 raft.EstadoRemoto) bool {
	leader_count := 0

	if reply1.EsLider {
		leader_count++
		if reply1.MatchIndex[1] != reply2.CommitIndex ||
			reply1.MatchIndex[2] != reply3.CommitIndex {
			fmt.Printf("error en matchindex con lider 1\n")
			return false
		}
	}
	if reply2.EsLider {
		leader_count++
		if reply2.MatchIndex[0] != reply1.CommitIndex ||
			reply2.MatchIndex[2] != reply3.CommitIndex {
			fmt.Printf("error en matchindex con lider 2\n")
			return false
		}
	}
	if reply3.EsLider {
		leader_count++
		if reply3.MatchIndex[0] != reply1.CommitIndex ||
			reply3.MatchIndex[1] != reply2.CommitIndex {
			fmt.Printf("error en matchindex con lider 3\n")
			return false
		}
	}

	return (leader_count == 1)
}
