/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2015 SRI International

 This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 --------------------------------------------------------------------------------
 */
package spade.reporter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import spade.core.AbstractEdge;
import spade.core.AbstractReporter;
import spade.core.BloomFilter;
import spade.core.Settings;
import spade.edge.opm.Used;
import spade.edge.opm.WasDerivedFrom;
import spade.edge.opm.WasGeneratedBy;
import spade.edge.opm.WasTriggeredBy;
import spade.reporter.audit.ArtifactIdentity;
import spade.reporter.audit.ArtifactProperties;
import spade.reporter.audit.BatchReader;
import spade.reporter.audit.DescriptorManager;
import spade.reporter.audit.FileIdentity;
import spade.reporter.audit.MemoryIdentity;
import spade.reporter.audit.NamedPipeIdentity;
import spade.reporter.audit.NetworkSocketIdentity;
import spade.reporter.audit.SYSCALL;
import spade.reporter.audit.UnixSocketIdentity;
import spade.reporter.audit.UnknownIdentity;
import spade.reporter.audit.UnnamedPipeIdentity;
import spade.utility.BerkeleyDB;
import spade.utility.CommonFunctions;
import spade.utility.Execute;
import spade.utility.ExternalMemoryMap;
import spade.utility.ExternalStore;
import spade.utility.FileUtility;
import spade.utility.Hasher;
import spade.vertex.opm.Artifact;
import spade.vertex.opm.Process;

/**
 *
 * IMPORTANT NOTE: To output OPM objects just once, use putProcess and putArtifact functions 
 * because they take care of that using internal data structures but more importantly those functions
 * contains the logic on how to create processes and artifacts which MUST be kept consistent across a run.
 *
 * @author Dawood Tariq, Sharjeel Ahmed Qureshi
 */
public class Audit extends AbstractReporter {

    static final Logger logger = Logger.getLogger(Audit.class.getName());

    /************************** Audit constants - START */
    
    private static final String SPADE_ROOT = Settings.getProperty("spade_root");
    
//  Just a human-friendly renaming of null
    private final static String NOT_SET = null;
    
//  Added to indicate in the output from where the OPM annotations were gotten. 
//  Either from 1) /proc or directly from 2) audit log or 3) beep instrumented kill syscall. 
    private static final String SOURCE = "source",
            PROC_FS = "/proc",
            DEV_AUDIT = "/dev/audit",
            BEEP = "beep";
    
//  Event id annotation key for OPM edges
    private final static String EVENT_ID = "event id";
    
//  Time in milliseconds to wait for the input log reader thread while checking for kernel to empty the reporter's buffer  
    private final long BUFFER_DRAIN_DELAY = 500;
//  Time in milliseconds to wait for threads to complete running their code when shutdown is called
    private final long THREAD_CLEANUP_TIMEOUT = 1000;
    
    /************************** Audit constants - END */
    
    /************************** Unix constants - START */
    
//  Following constant values are taken from:
//  http://lxr.free-electrons.com/source/include/uapi/linux/sched.h 
//  AND  
//  http://lxr.free-electrons.com/source/include/uapi/asm-generic/signal.h
    private final int SIGCHLD = 17, CLONE_VFORK = 0x00004000, CLONE_VM = 0x00000100;
    
//  Following constant values are taken from:
//  http://lxr.free-electrons.com/source/include/uapi/linux/stat.h#L14
    private final int S_IFIFO = 0010000, S_IFREG = 0100000, S_IFSOCK = 0140000;
  
//  Following constant values are taken from:
//  http://lxr.free-electrons.com/source/include/uapi/linux/fcntl.h#L56
    private final int AT_FDCWD = -100;
  
//  Following constant values are taken from:
//  http://lxr.free-electrons.com/source/include/uapi/asm-generic/fcntl.h#L19
    private final int O_RDONLY = 00000000, O_WRONLY = 00000001, O_RDWR = 00000002, O_CREAT = 00000100, O_TRUNC = 00001000;
  
    /************************** Unix constants - END */
    
    /************************** Audit log record parsing patterns - START */
    
    // Group 1: key
    // Group 2: value
    private static final Pattern pattern_key_value = Pattern.compile("(\\w+)=\"*((?<=\")[^\"]+(?=\")|([^\\s]+))\"*");

    // Group 1: node
    // Group 2: type
    // Group 3: time
    // Group 4: eventid
    private static final Pattern pattern_message_start = Pattern.compile("(?:node=(\\S+) )?type=(.+) msg=audit\\(([0-9\\.]+)\\:([0-9]+)\\):\\s*");

    // Group 1: cwd
    // CWD is either a quoted string or an unquoted string in which case it is in hex format
    private static final Pattern pattern_cwd = Pattern.compile("cwd=(\".+\"|[a-zA-Z0-9]+)");

    // Group 1: item number
    // Group 2: name
    // Group 3: nametype
    // Name is either a quoted string or an unquoted string in which case it is in hex format
    private static final Pattern pattern_path = Pattern.compile("item=([0-9]*) name=(\".+\"|[a-zA-Z0-9]+) .*nametype=([a-zA-Z]*)");
    
    /************************** Audit log record parsing patterns - END */
    
    /************************** Audit nobs to control behavior - START */

//  To toggle monitoring of IO syscalls on files and pipes
    private boolean USE_READ_WRITE = false;
//	To toggle monitoring of IO syscalls on network sockets and unix sockets
    private boolean USE_SOCK_SEND_RCV = false;
//  To toggle writing out of audit log that is being read
    private boolean DEBUG_DUMP_LOG = false;
//  To toggle creation of beep units
    private boolean CREATE_BEEP_UNITS = false;
//  To toggle simplification of OPM annotations. Process annotations and operation annotation on edges  
    private boolean SIMPLIFY = true;
//  To toggle building of initial process tree by reading /proc in case of live audit
    private boolean PROCFS = false;
//  To toggle sorting of audit log file provided in arguments
    private boolean SORTLOG = true;
//  To toggle versioning of network sockets
    private boolean NET_SOCKET_VERSIONING = false;
//  To toggle monitoring of unix sockets
    private boolean UNIX_SOCKETS = false;
//  To allow input audit log to be completely processed even if shutdown has been called
    private boolean WAIT_FOR_LOG_END = false;
//  To toggle monitoring of mmap, mmap2 and mprotect syscalls
    private boolean USE_MEMORY_SYSCALLS = true;
//  To toggle monitoring of system calls that only failed or only succeed. Valid values: "0", "1".
    private String AUDITCTL_SYSCALL_SUCCESS_FLAG = "1";
//  To toggle loading a saved state on launch for the reporter
    private boolean LOAD_STATE = false;
//  To toggle saving of state on shutdown for the reporter
    private boolean SAVE_STATE = false;
    
    /************************** Audit nobs to control behavior - START */
    
    /************************** Audit globals */
    
//  Flag to let threads know that shutdown has been called and should stop doing what they are doing
    private volatile boolean shutdown = false;
//  Read from system and then used to calculate start time of processes when reading process information from /proc. Only used for live audit.
    private long boottime = 0;
//  Flag used to identify architecture to get system call from system call number. 
//  Set from system in case of live audit.
//  Set from user argument in case of input audit log file.
    private Boolean ARCH_32BIT = true;
//  Store log for debugging purposes. Default value below but can be overridden using user argument.
    private String DEBUG_DUMP_FILE = SPADE_ROOT + "log/LinuxAudit.log"; //make sure SPADE_ROOT has been initialized before this
//  The path to binary that reads audit log records from audispd socket
    private String AUDIT_EXEC_PATH = SPADE_ROOT + "lib/spadeSocketBridge"; //make sure SPADE_ROOT has been initialized before this
//  Cache maps paths. global so that we can caches delete on exit
    private String eventBufferCacheDatabasePath, artifactsCacheDatabasePath;
    
//  The event reader from the spadeSocketBridge in case of Live Audit  
    private BufferedReader eventReader;
//  Event processor thread used to read audit log from spadeSocketBridge
    private Thread eventProcessorThread = null;
//  Thread to read from the input log file.
    private Thread auditLogThread = null;
//  Path for input audit log file to read from
    private String inputAuditLogFile = null;
//  A writer for the debug dump file
    private BufferedWriter dumpWriter = null;

//  Timestamp that is used to identify when the time has changed in unit begin. Assumes that the timestamps are received in sorted order
    private String lastTimestamp = NOT_SET; 
    
//  Map<pid, list of process vertices>. First vertex in the list of processes is always the containing process vertex.
//  Active unit process vertices are stacked on top previous vertices.
    private Map<String, LinkedList<Process>> processUnitStack = new HashMap<String, LinkedList<Process>>();
//  Map<pid, Map<unitId, iterationNumber>>. To keep count of unit begins (i.e. iteration begins) until unit end (i.e.
//  loop end) is seen.
    private Map<String, Map<String, Long>> iterationNumber = new HashMap<String, Map<String, Long>>();
//  A map to keep track of counts of unit vertex with the same pid, unitid and iteration number in the same timestamp
    private Map<UnitVertexIdentifier, Integer> unitIterationRepetitionCounts = new HashMap<UnitVertexIdentifier, Integer>();
//  Map for keeping memory addresses under use by a pid as instrumented by beep kill syscalls
    private Map<String, BigInteger> pidToMemAddress = new HashMap<String, BigInteger>(); 

//  A manager for file descriptors of processes
    private DescriptorManager descriptors = new DescriptorManager();
//  Map for artifact identities to artifact properties 
    private ExternalMemoryMap<ArtifactIdentity, ArtifactProperties> artifactIdentityToArtifactProperties;
    
//  Event buffer map based on Map<eventId, Map<key, value>>
    private ExternalMemoryMap<String, HashMap<String, String>> eventBuffer;
    
//  A hashset to keep track of seen record types which aren't handled by Audit reporter yet. 
//  Added to avoid output of redundant messages to spade log. Just keeps the type of the unhandled audit records.
    private final Set<String> seenTypesOfUnsupportedRecords = new HashSet<String>();
    
    public static void main(String[] args) throws Exception{
    	String newDir = "newdir";
    	String oldDir = "old/dir/e";
    	FileUtils.forceMkdir(new File(oldDir));
    	FileUtils.write(new File(oldDir + File.separator + "a.txt"), "helloworld");
    	FileUtils.moveDirectoryToDirectory(new File(oldDir), new File(newDir), true);
    	System.out.println(new File(new File(newDir).getAbsolutePath() + File.separator + new File(oldDir).getName()).exists());
    }
      
    /**
     * Checks if the map contains all the keys
     * 
     * @param log true if log message else false
     * @param map the map to check keys in
     * @param keys the keys to check in the map
     * @return true if all keys exist else false. if parameters null then false
     */
    private <X> boolean mapContainsAllKeys(boolean log, Map<X,?> map, @SuppressWarnings("unchecked") X... keys){
    	if(keys == null){
    		if(log){
    			logger.log(Level.WARNING, "keys are null");
    		}
    		return false;
    	}
    	if(map == null){
    		if(log){
    			logger.log(Level.WARNING, "map is null");
    		}
    		return false;
    	}
		boolean containsAll = true;
		for(X key : keys){
			if(!map.containsKey(key)){
				if(log){
					logger.log(Level.WARNING, key + " not defined.");
				}    				
				containsAll = false;
			}
		}
		return containsAll;
    }
    
    /*
     * Things to save and load (in order):
     * 1) descriptors
     * 2) pidToMemAddress
     * 3) unitIterationRepetitionCounts
     * 4) iterationNumber
     * 5) processUnitStack
     * 6) lastTimestamp
     * 7) eventBuffer.bloomFilter
     * 8) eventBuffer.keyHasher
     * 9) artifactIdentityToArtifactProperties.bloomFilter
     * 10) artifactIdentityToArtifactProperties.keyHasher
     */
    
    @SuppressWarnings("unchecked")
	private boolean loadState(Map<String, String> auditConfig){
    	ObjectInputStream objectReader = null;
    	try{
    		if(!mapContainsAllKeys(true, auditConfig, "eventBufferDatabaseName", "artifactsDatabaseName",
    				"eventBufferCacheSize", "artifactsCacheSize", "cacheDatabasePath", "savedEventBufferDatabasePath",
    				"savedArtifactsDatabasePath", "savedStateFile")){
    			logger.log(Level.SEVERE, "Failed to load state because missing keys in audit config.");
    			return false;
    		}
    		//read properties from audit config
    		String eventsDBName = auditConfig.get("eventBufferDatabaseName");
    		String artifactsDBName = auditConfig.get("artifactsDatabaseName");
    		
    		Integer eventsCacheMaxSize = CommonFunctions.parseInt(auditConfig.get("eventBufferCacheSize"), 0);
    		Integer artifactsCacheMaxSize = CommonFunctions.parseInt(auditConfig.get("artifactsCacheSize"), 0);
    		    		
    		String cacheDBDir = auditConfig.get("cacheDatabasePath");
    		String savedEventsDBDir = auditConfig.get("savedEventBufferDatabasePath");
    		String savedArtifactsDBDir = auditConfig.get("savedArtifactsDatabasePath");
    		
    		//read objects
    		objectReader = new ObjectInputStream(new FileInputStream(auditConfig.get("savedStateFile")));
    		descriptors = (DescriptorManager)objectReader.readObject();
    		pidToMemAddress = (Map<String, BigInteger>)objectReader.readObject();
    		unitIterationRepetitionCounts = (Map<UnitVertexIdentifier, Integer>)objectReader.readObject();
    		iterationNumber = (Map<String, Map<String, Long>>)objectReader.readObject();
    		processUnitStack = (Map<String, LinkedList<Process>>)objectReader.readObject();
    		lastTimestamp = (String)objectReader.readObject();
        	//external memory map object
    		BloomFilter<String> eventsBloomFilter = (BloomFilter<String>)objectReader.readObject();
    		Hasher<String> eventsKeyHasher = (Hasher<String>)objectReader.readObject();
    		BloomFilter<ArtifactIdentity> artifactsBloomFilter = (BloomFilter<ArtifactIdentity>)objectReader.readObject();
    		Hasher<ArtifactIdentity> artifactsKeyHasher = (Hasher<ArtifactIdentity>)objectReader.readObject();
    		
    		//initialize external maps
    		
    		//create paths for the new directories
    		eventBufferCacheDatabasePath = new File(cacheDBDir).getAbsolutePath() + File.separator + new File(savedEventsDBDir).getName();
    		artifactsCacheDatabasePath = new File(cacheDBDir).getAbsolutePath() + File.separator + new File(savedArtifactsDBDir).getName();
    		
    		//delete the directories if they exist before moving to this path
    		FileUtils.deleteQuietly(new File(eventBufferCacheDatabasePath));
    		FileUtils.deleteQuietly(new File(artifactsCacheDatabasePath));
    		
    		//moving whole directories instead of just copying files inside the directory
    		FileUtils.moveDirectoryToDirectory(new File(savedEventsDBDir), new File(cacheDBDir), true);
    		FileUtils.moveDirectoryToDirectory(new File(savedArtifactsDBDir), new File(cacheDBDir), true);
    		
    		//load external memory maps
    		ExternalStore<HashMap<String, String>> eventsExternalStore = 
    				new BerkeleyDB<HashMap<String, String>>(eventBufferCacheDatabasePath, eventsDBName, false);
    		ExternalStore<ArtifactProperties> artifactsExternalStore = 
    				new BerkeleyDB<ArtifactProperties>(artifactsCacheDatabasePath, artifactsDBName, false);
    		    		
    		eventBuffer = 
    				new ExternalMemoryMap<String, HashMap<String, String>>(eventsCacheMaxSize, eventsExternalStore, eventsBloomFilter);
    		eventBuffer.setKeyHashFunction(eventsKeyHasher);
    		
    		artifactIdentityToArtifactProperties = 
    				new ExternalMemoryMap<ArtifactIdentity, ArtifactProperties>(artifactsCacheMaxSize, artifactsExternalStore, artifactsBloomFilter);
    		artifactIdentityToArtifactProperties.setKeyHashFunction(artifactsKeyHasher);
    		    		
    		return true;
    	}catch(Exception e){
    		logger.log(Level.SEVERE, "Failed to load saved state", e);
    		return false;
    	}finally{
    		if(objectReader != null){
    			try{
    				objectReader.close();
    			}catch(Exception e){
    				logger.log(Level.SEVERE, "Failed to close object reader", e);
    			}
    		}
    	}
    }
    
    private boolean saveState(Map<String, String> auditConfig){
    	ObjectOutputStream objectWriter = null;
    	try{
    		File savedStateFile = null;
    		if(auditConfig.get("savedStateFile") == null){
    			logger.log(Level.SEVERE, "Undefined savedStateFile key. Failed to save state.");
    			return false;
    		}else{
    			savedStateFile = new File(auditConfig.get("savedStateFile"));
    		}		
        	//copy database to predefined directory.  make sure that the directory hasn't been deleted
        	String dirToSaveEventDBAt = auditConfig.get("savedEventBufferDatabasePath");
        	String dirToSaveArtifactDBAt = auditConfig.get("savedArtifactsDatabasePath");
        	
        	if(dirToSaveEventDBAt == null){
        		logger.log(Level.SEVERE, "savedEventBufferDatabasePath key not defined in config. Save state failed.");
        		return false;
        	}
        	
        	if(dirToSaveArtifactDBAt == null){
        		logger.log(Level.SEVERE, "savedArtifactsDatabasePath key not defined in config. Save state failed.");
        		return false;
        	}
        	
        	File dirFileToSaveEventDBAt = new File(dirToSaveEventDBAt);
        	File dirFileToSaveArtifactDBAt = new File(dirToSaveArtifactDBAt);
        	
        	//delete old state if any
        	FileUtils.deleteQuietly(dirFileToSaveEventDBAt);
        	FileUtils.deleteQuietly(dirFileToSaveArtifactDBAt);
        	
        	//create new state directories
        	FileUtils.forceMkdir(dirFileToSaveEventDBAt);
        	FileUtils.forceMkdir(dirFileToSaveArtifactDBAt);
        	
        	//copy database to the newly created predefined directories
        	FileUtils.copyDirectory(new File(eventBufferCacheDatabasePath), dirFileToSaveEventDBAt);
        	FileUtils.copyDirectory(new File(artifactsCacheDatabasePath), dirFileToSaveArtifactDBAt);
        	
        	//if audit dir in cfg dir doesn't exist
        	if(!savedStateFile.getParentFile().exists()){
        		FileUtils.forceMkdir(savedStateFile.getParentFile());
        	}
        	
        	objectWriter = new ObjectOutputStream(new FileOutputStream(savedStateFile));
        	objectWriter.writeObject(descriptors);
        	objectWriter.writeObject(pidToMemAddress);
        	objectWriter.writeObject(unitIterationRepetitionCounts);
        	objectWriter.writeObject(iterationNumber);
        	objectWriter.writeObject(processUnitStack);
        	objectWriter.writeObject(lastTimestamp);
        	//external memory maps. 
        	objectWriter.writeObject(eventBuffer.getBloomFilter());
        	objectWriter.writeObject(eventBuffer.getKeyHashFunction());
        	objectWriter.writeObject(artifactIdentityToArtifactProperties.getBloomFilter());
        	objectWriter.writeObject(artifactIdentityToArtifactProperties.getKeyHashFunction());
        	return true;
    	}catch(Exception e){
    		logger.log(Level.SEVERE, "Failed to save state", e);
    		return false;
    	}finally{
    		if(objectWriter != null){
    			try{
    				objectWriter.close();
    			}catch(Exception e){
    				logger.log(Level.SEVERE, "Failed to close object writer", e);
    			}
    		}
    	}
    }
    
    @Override
    public boolean launch(String arguments) {

        arguments = arguments == null ? "" : arguments;

        try {
            InputStream archStream = Runtime.getRuntime().exec("uname -i").getInputStream();
            BufferedReader archReader = new BufferedReader(new InputStreamReader(archStream));
            String archLine = archReader.readLine().trim();
            ARCH_32BIT = archLine.equals("i686");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error reading the system architecture", e);
            return false; //if unable to find out the architecture then report failure
        }

        Map<String, String> args = parseKeyValPairs(arguments);
        if (args.containsKey("outputLog")) {
            DEBUG_DUMP_LOG = true;
            if (!args.get("outputLog").isEmpty()) {
                DEBUG_DUMP_FILE = args.get("outputLog");
            }
            try{
            	dumpWriter = new BufferedWriter(new FileWriter(DEBUG_DUMP_FILE));
            }catch(Exception e){
            	logger.log(Level.WARNING, "Failed to create output log writer. Continuing...", e);
            }
        } else {
            DEBUG_DUMP_LOG = false;
        }
        
        // Check if file IO and net IO is also asked by the user to be turned on
        if ("true".equals(args.get("fileIO"))) {
            USE_READ_WRITE = true;
        }
        if ("true".equals(args.get("netIO"))) {
            USE_SOCK_SEND_RCV = true;
        }
        if("true".equals(args.get("units"))){
        	CREATE_BEEP_UNITS = true;
        }
        
        if("false".equals(args.get("sortLog"))){
        	SORTLOG = false;
        }
        if("true".equals(args.get("loadState"))){
        	LOAD_STATE = true;
        }
        if("true".equals(args.get("saveState"))){
        	SAVE_STATE = true;
        }
        
        // Arguments below are only for experimental use
        if("false".equals(args.get("simplify"))){
        	SIMPLIFY = false;
        }
        if("true".equals(args.get("procFS"))){
        	PROCFS = true;
        }
        if("true".equals(args.get("unixSockets"))){
        	UNIX_SOCKETS = true;
        }
        if("true".equals(args.get("netSocketVersioning"))){
        	NET_SOCKET_VERSIONING = true;
        }
        if("true".equals(args.get("waitForLog"))){
        	WAIT_FOR_LOG_END = true;
        }
        if("false".equals(args.get("memorySyscalls"))){
        	USE_MEMORY_SYSCALLS = false;
        }
        if("0".equals(args.get("auditctlSuccessFlag"))){
        	AUDITCTL_SYSCALL_SUCCESS_FLAG = "0";
        }
        // End of experimental arguments

//      initialize or load cache data structures
        try{
	        Map<String, String> auditConfig = FileUtility.readConfigFileAsKeyValueMap(Settings.getDefaultConfigFilePath(Audit.class), "=");
	        if(LOAD_STATE){
	        	if(!loadState(auditConfig)){
	        		return false;
	        	}
	        }else{
		        if(!initCacheMaps()){
		        	return false;
		        }
	        }
        }catch(Exception e){
        	logger.log(Level.SEVERE, "Failed to read config file", e);
        	return false;
        }
        
        // Get system boot time from /proc/stat. This is later used to determine
        // the start time for processes.
        try {
            BufferedReader boottimeReader = new BufferedReader(new FileReader("/proc/stat"));
            String line;
            while ((line = boottimeReader.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(line);
                if (st.nextToken().equals("btime")) {
                    boottime = Long.parseLong(st.nextToken()) * 1000;
                    break;
                }
            }
            boottimeReader.close();
        } catch (IOException | NumberFormatException e) {
            logger.log(Level.WARNING, "Error reading boot time information from /proc/", e);
        }
        
        inputAuditLogFile = args.get("inputLog");
        if(inputAuditLogFile != null){ //if a path is passed but it is not a valid file then throw an error
        	
        	if(!new File(inputAuditLogFile).exists()){
        		logger.log(Level.SEVERE, "Input audit log file at specified path doesn't exist.");
        		return false;
        	}
        	        	
        	ARCH_32BIT = null;
        	
        	if("32".equals(args.get("arch"))){
        		ARCH_32BIT = true;
        	}else if("64".equals(args.get("arch"))){
        		ARCH_32BIT = false;
        	}
        	
        	if(ARCH_32BIT == null){
        		logger.log(Level.SEVERE, "Must specify whether the system on which log was collected was 32 bit or 64 bit");
        		return false;
        	}
        	
        	if(SORTLOG){
        		try{
        			String sortedInputAuditLog = inputAuditLogFile + "." + System.currentTimeMillis();
        			String sortCommand = SPADE_ROOT + "bin/sortAuditLog " + inputAuditLogFile + " " + sortedInputAuditLog;
        			logger.log(Level.INFO, "Sorting audit log file '"+inputAuditLogFile+"' using command '"+sortCommand+"'");
        			List<String> output = Execute.getOutput(sortCommand);
        			logger.log(Level.INFO, output.toString());
        			
        			inputAuditLogFile = sortedInputAuditLog;
        			if(!new File(inputAuditLogFile).exists()){
                		logger.log(Level.SEVERE, "Failed to write sorted file to '"+inputAuditLogFile+"'");
                		return false;
                	}else{
                		logger.log(Level.INFO, "File sorted successfully");
                	}
        			
        		}catch(Exception e){
        			logger.log(Level.SEVERE, "Failed to sort input audit log file at '"+inputAuditLogFile+"'", e);
        			return false;
        		}
        	}
        	        	
        	auditLogThread = new Thread(new Runnable(){
    			public void run(){
    				BatchReader inputLogReader = null;
    	        	try{
    	        		inputLogReader = new BatchReader(new BufferedReader(new FileReader(inputAuditLogFile)));
    	        		String line = null;
    	        		while((!shutdown || WAIT_FOR_LOG_END) && (line = inputLogReader.readLine()) != null){
    	        			parseEventLine(line);
    	        		}
    	        		
    	        		// Either the reporter has been shutdown or the log has been ingested
    	        		boolean printed = false;

        	        	while(!shutdown){
        	        		if(!printed && getBuffer().size() == 0){//buffer processed
        	        			printed = true;
        	        			logger.log(Level.INFO, "Audit log processing succeeded: " + inputAuditLogFile);
        	        		}
        	        		try{
        	        			Thread.sleep(BUFFER_DRAIN_DELAY);
        	        		}catch(Exception e){
        	        			//logger.log(Level.SEVERE, null, e);
        	        		}
    					}
        	        	if(!printed){
        	        		logger.log(Level.INFO, "Audit log processing succeeded: " + inputAuditLogFile);
        	        	}
    	        	}catch(Exception e){
    	        		logger.log(Level.SEVERE, "Audit log processing failed: " + inputAuditLogFile, e);
    	        	}finally{
    	        		try{
    	        			if(inputLogReader != null){
    	        				inputLogReader.close();
    	        			}
    	        		}catch(Exception e){
    	        			logger.log(Level.WARNING, "Failed to close audit input log reader", e);
    	        		}
    	        		postProcessingCleanup();
    	        	}
    			}
    		});
        	auditLogThread.start();
        	
        }else{
        	
        	buildProcFSTree();

	        try {
	            // Start auditd and clear existing rules.
	            Runtime.getRuntime().exec("auditctl -D").waitFor();
	            Runnable eventProcessor = new Runnable() {
	                public void run() {
	                    try {
	                        java.lang.Process auditProcess = Runtime.getRuntime().exec(AUDIT_EXEC_PATH);
	                        eventReader = new BufferedReader(new InputStreamReader(auditProcess.getInputStream()));
	                        while (!shutdown) {
	                            String line = eventReader.readLine();
	                            if ((line != null) && !line.isEmpty()) {
	                                parseEventLine(line);
	                            }
	                        }
	                        //Added this command here because once the spadeSocketBridge process has exited any rules involving it cannot be cleared.
	                        //So, deleting the rules before destroying the spadeSocketBridge process.
	                        Runtime.getRuntime().exec("auditctl -D").waitFor();
	                        eventReader.close();
	                        auditProcess.destroy();
	                    } catch (IOException | InterruptedException e) {
	                        logger.log(Level.SEVERE, "Error launching main runnable thread", e);
	                    }finally{
	                    	postProcessingCleanup();
	                    }
	                }
	            };
	            eventProcessorThread = new Thread(eventProcessor, "Audit-Thread");
	            eventProcessorThread.start();
	
	            String auditRuleWithoutSuccess = "-a exit,always ";
	            String auditRulesWithSuccess = "-a exit,always ";
	            
	            if (ARCH_32BIT){
	            	auditRulesWithSuccess += "-F arch=b32 ";
	            	auditRuleWithoutSuccess += "-F arch=b32 ";
	            }else{
	            	auditRulesWithSuccess += "-F arch=b64 ";
	            	auditRuleWithoutSuccess += "-F arch=b64 ";
	            }
	            
	            auditRuleWithoutSuccess += "-S kill -S exit -S exit_group ";
	            
	            if (USE_READ_WRITE) {
	            	auditRulesWithSuccess += "-S read -S readv -S write -S writev ";
	            }
	            if (USE_SOCK_SEND_RCV) {
	            	auditRulesWithSuccess += "-S sendto -S recvfrom -S sendmsg -S recvmsg ";
	            }
	            if (USE_MEMORY_SYSCALLS) {
	            	auditRulesWithSuccess += "-S mmap -S mprotect ";
	            	if(ARCH_32BIT){
	            		auditRulesWithSuccess += "-S mmap2 ";
	            	}
	            }
	            auditRulesWithSuccess += "-S link -S symlink ";
	            auditRulesWithSuccess += "-S clone -S fork -S vfork -S execve ";
	            auditRulesWithSuccess += "-S open -S close -S creat -S openat -S mknodat -S mknod ";
	            auditRulesWithSuccess += "-S dup -S dup2 -S dup3 ";
	            auditRulesWithSuccess += "-S bind -S accept -S accept4 -S connect ";
	            auditRulesWithSuccess += "-S rename ";
	            auditRulesWithSuccess += "-S setuid -S setreuid -S setresuid ";
	            auditRulesWithSuccess += "-S chmod -S fchmod ";
	            auditRulesWithSuccess += "-S pipe -S pipe2 ";
	            auditRulesWithSuccess += "-S truncate -S ftruncate ";
	            auditRulesWithSuccess += "-F success=" + AUDITCTL_SYSCALL_SUCCESS_FLAG + " ";
	            
	            //Find the pids of the processes to ignore (below) and the uid for the JVM
	            /*
	             * All these fields would have been added to the main auditctl rule as "-F pid!=xxxx -F ppid!=xxxx" but 
	             * only 64 fields are allowed per each auditctl rule.
	             * 
	             * Add whatever number of fields that can be added to the main auditctl rule and add the rest individual rules as: 
	             *  "auditctl -a exit,never -F pid=xxxx" and "auditctl -a exit,never -F ppid=xxxx"
	             *  
	             * Adding the uid rule always first
	             */
	            
	            String uid = getOwnUid();
	            
	            if(uid == null){
	            	shutdown = true;
	            	return false;
	            }
	            
	            auditRulesWithSuccess += "-F uid!=" + uid + " ";
	            auditRuleWithoutSuccess += "-F uid!=" + uid + " ";
	            
	            String ignoreProcesses = "auditd kauditd audispd spadeSocketBridge";
	            List<String> pidsToIgnore = listOfPidsToIgnore(ignoreProcesses);
	            
	            int maxFieldsAllowed = 64; //max allowed by auditctl command
	            //split the pre-formed rule on -F to find out the number of fields already present
	            int existingFieldsCount = auditRulesWithSuccess.split(" -F ").length - 1; 
	            
	            //find out the pids & ppids that can be added to the main rule from the list of pids. divided by two to account for pid and ppid fields for the same pid
	            int fieldsForAuditRuleCount = (maxFieldsAllowed - existingFieldsCount)/2; 
	            
	            //handling the case if the main rule can accommodate all pids in the list of pids to ignore 
	            int loopFieldsForMainRuleTill = Math.min(fieldsForAuditRuleCount, pidsToIgnore.size());
	            
	            String fieldsForAuditRule = "";
	            //build the pid and ppid  to ignore portion for the main rule
	            for(int a = 0; a<loopFieldsForMainRuleTill; a++){
	            	fieldsForAuditRule += " -F pid!=" +pidsToIgnore.get(a) + " -F ppid!=" + pidsToIgnore.get(a); 
	            }
	            
	            //add the remaining pids as individual rules
	            for(int a = fieldsForAuditRuleCount; a<pidsToIgnore.size(); a++){
	            	String pidIgnoreAuditRule = "auditctl -a exit,never -F pid="+pidsToIgnore.get(a);
	            	if(!addAuditctlRule(pidIgnoreAuditRule)){
	            		shutdown = true;
	            		return false;
	            	}
	            	
	            	String ppidIgnoreAuditRule = "auditctl -a exit,never -F ppid="+pidsToIgnore.get(a);
	            	if(!addAuditctlRule(ppidIgnoreAuditRule)){
	            		shutdown = true;
	            		return false;
	            	}
	            }
	            
            	if(!addAuditctlRule(auditRuleWithoutSuccess + fieldsForAuditRule)){
            		shutdown = true;
            		return false;
            	}
            	
            	//add the main rule. ALWAYS ADD THIS AFTER THE ABOVE INDIVIDUAL RULES HAVE BEEN ADDED TO AVOID INCLUSION OF AUDIT INFO OF ABOVE PIDS
            	if(!addAuditctlRule(auditRulesWithSuccess + fieldsForAuditRule)){
            		shutdown = true;
            		return false;
            	}
            	
	        } catch (Exception e) {
	            logger.log(Level.SEVERE, "Error configuring audit rules", e);
	            shutdown = true;
	            return false;
	        }

        }
        
        return true;
    }
    
    /**
     * Called after audit event processor thread is done doing it's work.
     * Saves audit reporter state if flag was true and then deletes cache maps
     */
    private void postProcessingCleanup(){
    	try{
    		    		
    		eventBuffer.getExternalStore().shutdown();
    		artifactIdentityToArtifactProperties.getExternalStore().shutdown();
    		
    		if(SAVE_STATE){
    			Map<String, String> auditConfig = FileUtility.readConfigFileAsKeyValueMap(Settings.getDefaultConfigFilePath(Audit.class), "=");
    			saveState(auditConfig);
    		}
    		
    		if(eventBufferCacheDatabasePath != null && new File(eventBufferCacheDatabasePath).exists()){
    			FileUtils.forceDelete(new File(eventBufferCacheDatabasePath));
    		}
    		if(artifactsCacheDatabasePath != null && new File(artifactsCacheDatabasePath).exists()){
    			FileUtils.forceDelete(new File(artifactsCacheDatabasePath));
    		}
    	}catch(Exception e){
    		logger.log(Level.WARNING, "Failed to delete cache maps at paths: '"+eventBufferCacheDatabasePath+"' and '"+artifactsCacheDatabasePath+"'");
    	}
    }
    
    private boolean addAuditctlRule(String auditctlRule){
    	try{
    		List<String> auditctlOutput = Execute.getOutput("auditctl " + auditctlRule);
        	logger.log(Level.INFO, "configured audit rules: {0} with ouput: {1}", new Object[]{auditctlRule, auditctlOutput});
        	if(outputHasError(auditctlOutput)){
        		removeAuditctlRules();
        		shutdown = true;
        		return false;
        	}
        	return true;
    	}catch(Exception e){
    		logger.log(Level.SEVERE, "Failed to add auditctl rule = " + auditctlRule, e);
    		return false;
    	}
    }
    
    private void removeAuditctlRules(){
    	try{
    		logger.log(Level.INFO, "auditctl -D... output = " + Execute.getOutput("auditctl -D"));
    	}catch(Exception e){
    		logger.log(Level.SEVERE, "Failed to remove auditctl rules", e);
    	}
    }
    
    /**
     * Used to tell if the output of a command gotten from Execute.getOutput function has errors or not
     * 
     * @param outputLines output lines received from Execute.getOutput
     * @return true if errors exist, otherwise false
     */
    private boolean outputHasError(List<String> outputLines){
    	if(outputLines != null){
	    	for(String outputLine : outputLines){
	    		if(outputLine != null){
	    			if(outputLine.contains("[STDERR]")){
	    				return true;
	    			}
	    		}
	    	}
    	}
    	return false;
    }
    
    private void buildProcFSTree(){
    	if(PROCFS){
	        // Build the process tree using the directories under /proc/.
	        // Directories which have a numeric name represent processes.
	        String path = "/proc";
	        java.io.File directory = new java.io.File(path);
	        java.io.File[] listOfFiles = directory.listFiles();
	        for (int i = 0; i < listOfFiles.length; i++) {
	            if (listOfFiles[i].isDirectory()) {
	
	                String currentPID = listOfFiles[i].getName();
	                try {
	                    // Parse the current directory name to make sure it is
	                    // numeric. If not, ignore and continue.
	                    Integer.parseInt(currentPID);
	                    Process processVertex = createProcessFromProcFS(currentPID); //create
	                    addProcess(currentPID, processVertex);//add to memory
	                    putVertex(processVertex);//add to buffer
	                    Process parentVertex = getProcess(processVertex.getAnnotation("ppid"));
	                    if (parentVertex != null) {
	                    	putWasTriggeredByEdge(processVertex, parentVertex, "0", null, PROC_FS, getOperation(SYSCALL.UNKNOWN));
	                    }
	
	                    // Get existing file descriptors for this process
	                    Map<String, ArtifactIdentity> fds = getFileDescriptors(currentPID);
	                    if (fds != null) {
	                        descriptors.addDescriptors(currentPID, fds);
	                    }
	                } catch (Exception e) {
	                    // Continue
	                }
	            }
	        }
    	}
    }
    
    private boolean initCacheMaps(){
    	 try{
         	Map<String, String> configMap = FileUtility.readConfigFileAsKeyValueMap(Settings.getDefaultConfigFilePath(this.getClass()), "=");
         	long currentTime = System.currentTimeMillis(); 
             eventBufferCacheDatabasePath = configMap.get("cacheDatabasePath") + File.separatorChar + "eventbuffer_" + currentTime;
             artifactsCacheDatabasePath = configMap.get("cacheDatabasePath") + File.separatorChar + "artifacts_" + currentTime;
             try{
     	        FileUtils.forceMkdir(new File(eventBufferCacheDatabasePath));
     	        FileUtils.forceMkdir(new File(artifactsCacheDatabasePath));
     	        FileUtils.forceDeleteOnExit(new File(eventBufferCacheDatabasePath));
     	        FileUtils.forceDeleteOnExit(new File(artifactsCacheDatabasePath));
             }catch(Exception e){
             	logger.log(Level.SEVERE, "Failed to create cache database directories", e);
             	return false;
             }
             
             try{
             	Integer eventBufferCacheSize = CommonFunctions.parseInt(configMap.get("eventBufferCacheSize"), null);
             	String eventBufferDatabaseName = configMap.get("eventBufferDatabaseName");
             	Double eventBufferFalsePositiveProbability = CommonFunctions.parseDouble(configMap.get("eventBufferBloomfilterFalsePositiveProbability"), null);
             	Integer eventBufferExpectedNumberOfElements = CommonFunctions.parseInt(configMap.get("eventBufferBloomFilterExpectedNumberOfElements"), null);
             	
             	Integer artifactsCacheSize = CommonFunctions.parseInt(configMap.get("artifactsCacheSize"), null);
             	String artifactsDatabaseName = configMap.get("artifactsDatabaseName");
             	Double artifactsFalsePositiveProbability = CommonFunctions.parseDouble(configMap.get("artifactsBloomfilterFalsePositiveProbability"), null);
             	Integer artifactsExpectedNumberOfElements = CommonFunctions.parseInt(configMap.get("artifactsBloomFilterExpectedNumberOfElements"), null);
             	
             	logger.log(Level.INFO, "Audit cache properties: eventBufferCacheSize={0}, eventBufferDatabaseName={1}, "
             			+ "eventBufferBloomfilterFalsePositiveProbability={2}, eventBufferBloomFilterExpectedNumberOfElements={3}, "
             			+ "artifactsCacheSize={4}, artifactsDatabaseName={5}, artifactsBloomfilterFalsePositiveProbability={6}, "
             			+ "artifactsBloomFilterExpectedNumberOfElements={7}", new Object[]{eventBufferCacheSize, eventBufferDatabaseName,
             					eventBufferFalsePositiveProbability, eventBufferExpectedNumberOfElements, artifactsCacheSize, 
             					artifactsDatabaseName, artifactsFalsePositiveProbability, artifactsExpectedNumberOfElements});
             	
             	if(eventBufferCacheSize == null || eventBufferDatabaseName == null || eventBufferFalsePositiveProbability == null || 
             			eventBufferExpectedNumberOfElements == null || artifactsCacheSize == null || artifactsDatabaseName == null || 
             			artifactsFalsePositiveProbability == null || artifactsExpectedNumberOfElements == null){
             		logger.log(Level.SEVERE, "Undefined cache properties in Audit config");
             		return false;
             	}
             	
             	BloomFilter<String> eventBufferBloomFilter = null;
             	BloomFilter<ArtifactIdentity> artifactsBloomFilter = null;
             	
             	if(eventBufferFalsePositiveProbability < 0 || eventBufferFalsePositiveProbability > 1){
             		logger.log(Level.SEVERE, "Event buffer bloom filter false positive probability must be in the range [0-1]");
        			return false;
        		}
        		
             	if(artifactsFalsePositiveProbability < 0 || artifactsFalsePositiveProbability > 1){
             		logger.log(Level.SEVERE, "Artifacts bloom filter false positive probability must be in the range [0-1]");
        			return false;
        		}
             	
        		if(eventBufferExpectedNumberOfElements < 1){
        			logger.log(Level.SEVERE, "Event buffer bloom filter expected number of elements cannot be less than 1");
        			return false;
        		}
        		
        		if(artifactsExpectedNumberOfElements < 1){
        			logger.log(Level.SEVERE, "Artifacts bloom filter expected number of elements cannot be less than 1");
        			return false;
        		}
        		
        		eventBufferBloomFilter = new BloomFilter<String>(eventBufferFalsePositiveProbability, eventBufferExpectedNumberOfElements);
        		artifactsBloomFilter = new BloomFilter<ArtifactIdentity>(artifactsFalsePositiveProbability, artifactsExpectedNumberOfElements);
        		
             	eventBuffer = new ExternalMemoryMap<String, HashMap<String, String>>(eventBufferCacheSize, 
                 				new BerkeleyDB<HashMap<String, String>>(eventBufferCacheDatabasePath, eventBufferDatabaseName, true), 
                 				eventBufferBloomFilter);
                artifactIdentityToArtifactProperties = 
                 		new ExternalMemoryMap<ArtifactIdentity, ArtifactProperties>(artifactsCacheSize, 
                 				new BerkeleyDB<ArtifactProperties>(artifactsCacheDatabasePath, artifactsDatabaseName, true), 
                 				artifactsBloomFilter);
             }catch(Exception e){
             	logger.log(Level.SEVERE, "Failed to initialize necessary data structures", e);
             	return false;
             }
             
         }catch(Exception e){
         	logger.log(Level.SEVERE, "Failed to read default config file", e);
         	return false;
         }
    	 return true;
    }
    
    private List<String> listOfPidsToIgnore(String ignoreProcesses) {
    	
//    	ignoreProcesses argument is a string of process names separated by blank space
    	
    	List<String> pids = new ArrayList<String>();
        try {
        	if(ignoreProcesses != null && !ignoreProcesses.trim().isEmpty()){
//	            Using pidof command now to get all pids of the mentioned processes
	            java.lang.Process pidChecker = Runtime.getRuntime().exec("pidof " + ignoreProcesses);
//	            pidof returns pids of given processes as a string separated by a blank space
	            BufferedReader pidReader = new BufferedReader(new InputStreamReader(pidChecker.getInputStream()));
	            String pidline = pidReader.readLine();
//	            added all returned from pidof command
	            pids.addAll(Arrays.asList(pidline.split("\\s+")));
	            pidReader.close();
        	}
            
            return pids;
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error building list of processes to ignore. Partial list: " + pids, e);
            return new ArrayList<String>();
        }
    }
    
    private String getOwnUid(){
    	try{
    		String uid = null;
    		List<String> outputLines = Execute.getOutput("id -u");
    		for(String outputLine : outputLines){
    			if(outputLine != null){
	    			if(outputLine.contains("[STDOUT]")){
	    				uid = outputLine.replace("[STDOUT]", "").trim(); 
	    			}else if(outputLine.contains("[STDERR]")){
	    				logger.log(Level.SEVERE, "Failed to get user id of JVM. Output = " + outputLines);
	    				return null;
	    			}
    			}
    		}
    		return uid;
    	}catch(Exception e){
    		logger.log(Level.SEVERE, "Failed to get user id of JVM", e);
    		return null;
    	}
    }
    
	private Map<String, ArtifactIdentity> getFileDescriptors(String pid){
		
		if(auditLogThread != null  // the audit log is being read from a file.
				|| !PROCFS){ // the flag to read from procfs is false
			return null;
		}
    	
    	Map<String, ArtifactIdentity> fds = new HashMap<String, ArtifactIdentity>();
    	
    	Map<String, String> inodefd0 = new HashMap<String, String>();
    	
    	try{
    		//LSOF args -> n = no DNS resolution, P = no port user-friendly naming, p = pid of process
    		List<String> lines = Execute.getOutput("lsof -nPp " + pid);
    		if(lines != null && lines.size() > 1){
    			lines.remove(0); //remove the heading line
    			for(String line : lines){
    				String tokens[] = line.split("\\s+");
    				if(tokens.length >= 9){
    					String type = tokens[4].toLowerCase().trim();
    					String fd = tokens[3].trim();
    					fd = fd.replaceAll("[^0-9]", ""); //ends with r(read), w(write), u(read and write), W (lock)
    					if(CommonFunctions.parseInt(fd, null) != null){
	    					if("fifo".equals(type)){
	    						String path = tokens[8];
	    						if("pipe".equals(path)){ //unnamed pipe
	    							String inode = tokens[7];
		    						if(inodefd0.get(inode) == null){
		    							inodefd0.put(inode, fd);
		    						}else{
		    							ArtifactIdentity pipeInfo = new UnnamedPipeIdentity(pid, fd, inodefd0.get(inode));
		    							fds.put(fd, pipeInfo);
		    							fds.put(inodefd0.get(inode), pipeInfo);
		    							inodefd0.remove(inode);
		    						}
	    						}else{ //named pipe
	    							fds.put(fd, new NamedPipeIdentity(path));
	    						}	    						
	    					}else if("ipv4".equals(type) || "ipv6".equals(type)){
	    						String protocol = tokens[7];
	    						//example token 8 = 10.0.2.15:35859->172.231.72.152:443 (ESTABLISHED)
	    						String[] srchostport = tokens[8].split("->")[0].split(":");
	    						String[] dsthostport = tokens[8].split("->")[1].split("\\s+")[0].split(":");
	    						fds.put(fd, new NetworkSocketIdentity(srchostport[0], srchostport[1], dsthostport[0], dsthostport[1], protocol));
	    					}else if("reg".equals(type) || "chr".equals(type)){
	    						String path = tokens[8];
	    						fds.put(fd, new FileIdentity(path));  						
	    					}else if("unix".equals(type)){
	    						String path = tokens[8];
	    						if(!path.equals("socket")){
	    							fds.put(fd, new UnixSocketIdentity(path));
	    						}
	    					}
    					}
    				}
    			}
    		}
    	}catch(Exception e){
    		logger.log(Level.WARNING, "Failed to get file descriptors for pid " + pid, e);
    	}
    	
    	return fds;
    }
	
	/*
     * Takes a string with keyvalue pairs and returns a Map Input e.g.
     * "key1=val1 key2=val2" etc. Input string validation is callee's
     * responsiblity
     */
    private static Map<String, String> parseKeyValPairs(String messageData) {
        Matcher key_value_matcher = pattern_key_value.matcher(messageData);
        Map<String, String> keyValPairs = new HashMap<>();
        while (key_value_matcher.find()) {
            keyValPairs.put(key_value_matcher.group(1), key_value_matcher.group(2));
        }
        return keyValPairs;
    }

	//TODO What to do when WAIT_FOR_LOG_END is set to true and auditLogThread won't stop on exit?
    @Override
    public boolean shutdown() {
        shutdown = true;
        try {
        	if(dumpWriter != null){
        		dumpWriter.close();
        	}
        	if(eventProcessorThread != null){
        		eventProcessorThread.join(THREAD_CLEANUP_TIMEOUT);
        	}
        	if(auditLogThread != null){
        		auditLogThread.join(THREAD_CLEANUP_TIMEOUT);
        	}
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error shutting down", e);
        }
        return true;
    }
    
    private void parseEventLine(String line) {
    	
    	if (DEBUG_DUMP_LOG) {
    		try{
    			dumpWriter.write(line + System.getProperty("line.separator"));
            	dumpWriter.flush();
    		}catch(Exception e){
    			logger.log(Level.WARNING, "Failed to write to output log", e);
    		}
        }
    	
        Matcher event_start_matcher = pattern_message_start.matcher(line);
        if (event_start_matcher.find()) {
//            String node = event_start_matcher.group(1);
            String type = event_start_matcher.group(2);
            String time = event_start_matcher.group(3);
            String eventId = event_start_matcher.group(4);
            String messageData = line.substring(event_start_matcher.end());
            
            if(eventBuffer.get(eventId) == null){
            	eventBuffer.put(eventId, new HashMap<String, String>());
            	eventBuffer.get(eventId).put("eventid", eventId);
            }
            
            if (type.equals("SYSCALL")) {
                Map<String, String> eventData = parseKeyValPairs(messageData);
                eventData.put("time", time);
            	eventBuffer.get(eventId).putAll(eventData);
            } else if (type.equals("EOE")) {
                finishEvent(eventId);
            } else if (type.equals("CWD")) {
                Matcher cwd_matcher = pattern_cwd.matcher(messageData);
                if (cwd_matcher.find()) {
                    String cwd = cwd_matcher.group(1);
                    cwd = cwd.trim();
                    if(cwd.startsWith("\"") && cwd.endsWith("\"")){ //is a string path
                    	cwd = cwd.substring(1, cwd.length()-1);
                    }else{ //is in hex format
                    	cwd = parseHexStringToUTF8(cwd);
                    }                    
                    eventBuffer.get(eventId).put("cwd", cwd);
                }
            } else if (type.equals("PATH")) {
                Matcher path_matcher = pattern_path.matcher(messageData);
                if (path_matcher.find()) {
                    String item = path_matcher.group(1);
                    String name = path_matcher.group(2);
                    String nametype = path_matcher.group(3);
                    name = name.trim();
                    if(name.startsWith("\"") && name.endsWith("\"")){ //is a string path
                    	name = name.substring(1, name.length()-1);
                    }else{ //is in hex format
                    	name = parseHexStringToUTF8(name);
                    }
                    eventBuffer.get(eventId).put("path" + item, name);
                    eventBuffer.get(eventId).put("nametype" + item, nametype);
                }
            } else if (type.equals("EXECVE")) {
                Matcher key_value_matcher = pattern_key_value.matcher(messageData);
                while (key_value_matcher.find()) {
                    eventBuffer.get(eventId).put("execve_" + key_value_matcher.group(1), key_value_matcher.group(2));
                }
            } else if (type.equals("FD_PAIR")) {
                Matcher key_value_matcher = pattern_key_value.matcher(messageData);
                while (key_value_matcher.find()) {
                    eventBuffer.get(eventId).put(key_value_matcher.group(1), key_value_matcher.group(2));
                }
            } else if (type.equals("SOCKETCALL")) {
                Matcher key_value_matcher = pattern_key_value.matcher(messageData);
                while (key_value_matcher.find()) {
                    eventBuffer.get(eventId).put("socketcall_" + key_value_matcher.group(1), key_value_matcher.group(2));
                }
            } else if (type.equals("SOCKADDR")) {
                Matcher key_value_matcher = pattern_key_value.matcher(messageData);
                while (key_value_matcher.find()) {
                    eventBuffer.get(eventId).put(key_value_matcher.group(1), key_value_matcher.group(2));
                }
            } else if(type.equals("NETFILTER_PKT")){
            	Matcher key_value_matcher = pattern_key_value.matcher(messageData);
            	while (key_value_matcher.find()) {
                    eventBuffer.get(eventId).put(key_value_matcher.group(1), key_value_matcher.group(2));
                }
            	finishEvent(eventId);
            } else if (type.equals("MMAP")){
            	Matcher key_value_matcher = pattern_key_value.matcher(messageData);
                while (key_value_matcher.find()) {
                    eventBuffer.get(eventId).put(key_value_matcher.group(1), key_value_matcher.group(2));
                }
            } else if(type.equals("PROCTITLE")){
            	//record type not being handled at the moment. 
            } else {
            	if(!seenTypesOfUnsupportedRecords.contains(type)){
            		seenTypesOfUnsupportedRecords.add(type);
            		logger.log(Level.WARNING, "Unknown type {0} for message: {1}. Won't output to log a message for this type again.", new Object[]{type, line});
            	}                
            }
            
        } else {
            logger.log(Level.WARNING, "unable to match line: {0}", line);
        }
    }

    private void finishEvent(String eventId){

    	if (eventBuffer.get(eventId) == null) {
    		logger.log(Level.WARNING, "EOE for eventID {0} received with no prior Event Info", new Object[]{eventId});
    		return;
    	}

    	/*if("NETFILTER_PKT".equals(eventBuffer.get(eventId).get("type"))){ //for events with no syscalls
    		try{
    			handleNetfilterPacketEvent(eventBuffer.get(eventId));
    		}catch(Exception e){
    			logger.log(Level.WARNING, "Error processing finish syscall event with event id '"+eventId+"'", e);
    		}
    	}else{ //for events with syscalls
    		handleSyscallEvent(eventId);
    	}*/
    	
    	handleSyscallEvent(eventId);
    }
    
    /**
     * Gets the key value map from the internal data structure and gets the system call from the map.
     * Gets the appropriate system call based on current architecture
     * If global flag to log only successful events is set to true but the current event wasn't successful then only handle it if was either a kill
     * system call or exit system call or exit_group system call.
     * 
     * IMPORTANT: Converts all 4 arguments, a0 o a3 to decimal integers from hexadecimal integers and puts them back in the key value map
     * 
     * Calls the appropriate system call handler based on the system call
     * 
     * @param eventId id of the event against which the key value maps are saved
     */
    private void handleSyscallEvent(String eventId) {
    	try {

    		Map<String, String> eventData = eventBuffer.get(eventId);
    		int syscallNum = CommonFunctions.parseInt(eventData.get("syscall"), -1);

    		int arch = -1;
    		if(ARCH_32BIT){
    			arch = 32;
    		}else{
    			arch = 64;
    		}
    		
    		if(syscallNum == -1){
    			logger.log(Level.INFO, "A non-syscall audit event OR missing syscall record with for event with id '" + eventId + "'");
    			return;
    		}

    		SYSCALL syscall = SYSCALL.getSyscall(syscallNum, arch);

    		if("1".equals(AUDITCTL_SYSCALL_SUCCESS_FLAG) && "no".equals(eventData.get("success"))){
    			//if only log successful events but the current event had success no then only monitor the following calls.
    			if(syscall == SYSCALL.KILL || syscall == SYSCALL.EXIT || syscall == SYSCALL.EXIT_GROUP){
	    			//continue and log these syscalls irrespective of success
    			}else{ //for all others don't log
    				eventBuffer.remove(eventId);
	    			return;
    			}
    		}
    		
    		//convert all arguments from hexadecimal format to decimal format and replace them. done for convenience here and to avoid issues. 
    		for(int argumentNumber = 0; argumentNumber<4; argumentNumber++){ //only 4 arguments received from linux audit
    			try{
    				eventData.put("a"+argumentNumber, new BigInteger(eventData.get("a"+argumentNumber), 16).toString());
    			}catch(Exception e){
    				logger.log(Level.INFO, "Missing/Non-numerical argument#" + argumentNumber + " for event id '"+eventId+"'");
    			}
    		}

    		switch (syscall) {
    			case EXIT:
    			case EXIT_GROUP:
    				handleExit(eventData, syscall);
    				break;
	    		case READ: 
	    		case READV:
	    		case PREAD64:
	    		case WRITE: 
	    		case WRITEV:
	    		case PWRITE64:
	    		case SENDMSG: 
	    		case RECVMSG: 
	    		case SENDTO: 
	    		case RECVFROM: 
	    			handleIOEvent(syscall, eventData);
	    			break;
	    		case MMAP:
	    		case MMAP2:
	    			handleMmap(eventData, syscall);
	    			break;
	    		case MPROTECT:
	    			handleMprotect(eventData, syscall);
	    			break;
	    		case SYMLINK:
	    		case LINK:
	    			handleLink(eventData, syscall);
	    			break;	    			
	    		case VFORK:
	    		case FORK:
	    		case CLONE:
	    			handleForkClone(eventData, syscall);
	    			break;
	    		case EXECVE:
	    			handleExecve(eventData);
	    			break;
	    		case OPEN:
	    			handleOpen(eventData, syscall);
	    			break;
	    		case CLOSE:
	    			handleClose(eventData);
	    			break;
	    		case CREAT:
	    			handleCreat(eventData);
	    			break;
	    		case OPENAT:
	    			handleOpenat(eventData);
	    			break;
	    		case MKNODAT:
	    			handleMknodat(eventData);
	    			break;
	    		case MKNOD:
	    			handleMknod(eventData, syscall);
	    			break;
	    		case DUP:
	    		case DUP2:
	    		case DUP3:
	    			handleDup(eventData, syscall);
	    			break;
	    		case BIND:
	    			handleBind(eventData, syscall);
	    			break;
	    		case ACCEPT4:
	    		case ACCEPT:
	    			handleAccept(eventData, syscall);
	    			break;
	    		case CONNECT:
	    			handleConnect(eventData);
	    			break;
	    		case KILL:
	    			handleKill(eventData);
	    			break;
	    		case RENAME: 
	    			handleRename(eventData);
	    			break;
	    		case SETREUID:
	    		case SETRESUID:
	    		case SETUID:
	    			handleSetuid(eventData, syscall);
	    			break; 
	    		case CHMOD:
	    		case FCHMOD:
	    			handleChmod(eventData, syscall);
	    			break;
	    		case PIPE:
	    		case PIPE2:
	    			handlePipe(eventData, syscall);
	    			break;
	    		case TRUNCATE:
	    		case FTRUNCATE:
	    			handleTruncate(eventData, syscall);
	    			break;
//                case SOCKET: // socket()
//                    break;
	    		
	    		default: //SYSCALL.UNSUPPORTED
	    			logger.log(Level.WARNING, "Unsupported syscall '"+syscallNum+"' for eventid '" + eventId + "'");
    		}
    		eventBuffer.remove(eventId);
    	} catch (Exception e) {
    		logger.log(Level.WARNING, "Error processing finish syscall event with eventid '"+eventId+"'", e);
    	}
    }
    
    /**
     * Only for IO syscalls where the FD is argument 0 (a0).
     * If the descriptor is not a valid descriptor then handled as file or socket based on the syscall.
     * Otherwise handled based on artifact identity type
     * 
     * @param syscall system call
     * @param eventData audit event data gotten in the log
     */
    private void handleIOEvent(SYSCALL syscall, Map<String, String> eventData){
    	String pid = eventData.get("pid");
    	String fd = eventData.get("a0");
    	Class<? extends ArtifactIdentity> artifactIdentityClass = null;

    	if(descriptors.getDescriptor(pid, fd) != null){
    		artifactIdentityClass = descriptors.getDescriptor(pid, fd).getClass();
    	}
    	
    	if(artifactIdentityClass == null || UnknownIdentity.class.equals(artifactIdentityClass)){ //either a new unknown i.e. null or a previously seen unknown
    		if((syscall == SYSCALL.READ || syscall == SYSCALL.READV || syscall == SYSCALL.PREAD64 || syscall == SYSCALL.WRITE || syscall == SYSCALL.WRITEV || syscall == SYSCALL.PWRITE64)){
    			if(USE_READ_WRITE){
    				handleFileIOEvent(syscall, eventData);
    			}
    		}else if((syscall == SYSCALL.SENDMSG || syscall == SYSCALL.SENDTO || syscall == SYSCALL.RECVFROM || syscall == SYSCALL.RECVMSG)){
    			if(USE_SOCK_SEND_RCV){
    				handleNetworkIOEvent(syscall, eventData);
    			}
    		}else {
    			logger.log(Level.WARNING, "Unknown file descriptor type for eventid '"+eventData.get("eventid")+"' and syscall '"+syscall+"'");
    		}
    	}else if(NetworkSocketIdentity.class.equals(artifactIdentityClass) || UnixSocketIdentity.class.equals(artifactIdentityClass)){ 
    		handleNetworkIOEvent(syscall, eventData);
    	}else if(FileIdentity.class.equals(artifactIdentityClass) || MemoryIdentity.class.equals(artifactIdentityClass) 
    			|| UnnamedPipeIdentity.class.equals(artifactIdentityClass) || NamedPipeIdentity.class.equals(artifactIdentityClass)){
    		handleFileIOEvent(syscall, eventData);
    	}
    }

    private void handleNetworkIOEvent(SYSCALL syscall, Map<String, String> eventData){
    	if(USE_SOCK_SEND_RCV){
    		switch (syscall) {
	    		case WRITE: 
	    		case WRITEV: 
	    		case PWRITE64:
	    		case SENDTO:
	    		case SENDMSG:
	    			handleSend(eventData, syscall);
	    			break;
	    		case READ:
	    		case READV:
	    		case PREAD64:
	    		case RECVFROM:
	    		case RECVMSG:
	    			handleRecv(eventData, syscall);
	    			break;
	    		default:
	    			break;
    		}
    	}
    }

    private void handleFileIOEvent(SYSCALL syscall, Map<String, String> eventData){
    	if(USE_READ_WRITE){
    		switch(syscall){
	    		case READ:
	    		case READV:
	    		case PREAD64:
	    			handleRead(eventData, syscall);
	    			break;
	    		case WRITE:
	    		case WRITEV:
	    		case PWRITE64:
	    			handleWrite(eventData, syscall);
	    			break;
	    		default:
	    			break;
    		}
    	}
    }
    
    private void handleExit(Map<String, String> eventData, SYSCALL syscall){
    	// kill() receive the following message(s):
        // - SYSCALL
        // - EOE
    	
    	String pid = eventData.get("pid");
    	processUnitStack.remove(pid); //remove from memory
    }
    
    private void handleMmap(Map<String, String> eventData, SYSCALL syscall){
    	// mmap() receive the following message(s):
    	// - MMAP
        // - SYSCALL
        // - EOE
    	
    	if(!USE_MEMORY_SYSCALLS){
    		return;
    	}
    	
    	String pid = eventData.get("pid");
    	String time = eventData.get("time");
    	String address = new BigInteger(eventData.get("exit")).toString(16); //convert to hexadecimal
    	String length = new BigInteger(eventData.get("a1")).toString(16); //convert to hexadecimal
    	String protection = new BigInteger(eventData.get("a2")).toString(16); //convert to hexadecimal
    	String fd = eventData.get("fd");
    	
    	if(fd == null){
    		logger.log(Level.INFO, "FD record missing in mmap event. event id '"+eventData.get("eventid")+"'");
    		return;
    	}
    	
    	ArtifactIdentity fileArtifactIdentity = descriptors.getDescriptor(pid, fd);
    	
    	if(fileArtifactIdentity == null){
    		descriptors.addUnknownDescriptor(pid, fd);
    		getArtifactProperties(descriptors.getDescriptor(pid, fd)).markNewEpoch(eventData.get("eventid"));
    		fileArtifactIdentity = descriptors.getDescriptor(pid, fd);
    	}
    	
    	//if not unknown and not file
    	if((!UnknownIdentity.class.equals(fileArtifactIdentity.getClass()) && !FileIdentity.class.equals(fileArtifactIdentity.getClass()))){
    		logger.log(Level.INFO, "Syscall {0} only supported for unknown and file artifact types and not {1}. event id {2}",
    				new Object[]{syscall, fileArtifactIdentity.getClass(), eventData.get("eventid")});
    		return;
    	}
    	
    	Artifact fileArtifact = putArtifact(eventData, fileArtifactIdentity, false);
    	
    	ArtifactIdentity memoryArtifactIdentity = new MemoryIdentity(pid, address, length);
    	Artifact memoryArtifact = putArtifact(eventData, memoryArtifactIdentity, true);
		
		Process process = putProcess(eventData); //create if doesn't exist
		
		WasGeneratedBy wgbEdge = new WasGeneratedBy(memoryArtifact, process);
		wgbEdge.addAnnotation("time", time);
		wgbEdge.addAnnotation("operation", getOperation(syscall)+"_"+getOperation(SYSCALL.WRITE));
		addEventIdAndSourceAnnotationToEdge(wgbEdge, eventData.get("eventid"), DEV_AUDIT);
		
		Used usedEdge = new Used(process, fileArtifact);
		usedEdge.addAnnotation("time", time);
		usedEdge.addAnnotation("operation", getOperation(syscall)+"_"+getOperation(SYSCALL.READ));
		addEventIdAndSourceAnnotationToEdge(usedEdge, eventData.get("eventid"), DEV_AUDIT);
		
		WasDerivedFrom wdfEdge = new WasDerivedFrom(memoryArtifact, fileArtifact);
		wdfEdge.addAnnotation("protection", protection);
		wdfEdge.addAnnotation("time", time);
		wdfEdge.addAnnotation("operation", getOperation(syscall));
		wdfEdge.addAnnotation("pid", pid);
		addEventIdAndSourceAnnotationToEdge(wdfEdge, eventData.get("eventid"), DEV_AUDIT);
		
		putEdge(wdfEdge);
		putEdge(wgbEdge);
		putEdge(usedEdge);
    }
    
    private void handleMprotect(Map<String, String> eventData, SYSCALL syscall){
    	// mprotect() receive the following message(s):
        // - SYSCALL
        // - EOE
    	
    	if(!USE_MEMORY_SYSCALLS){
    		return;
    	}
    	
    	String pid = eventData.get("pid");
    	String time = eventData.get("time");
    	String address = new BigInteger(eventData.get("a0")).toString(16);
    	String length = new BigInteger(eventData.get("a1")).toString(16);
    	String protection = new BigInteger(eventData.get("a2")).toString(16);
    	
    	ArtifactIdentity memoryInfo = new MemoryIdentity(pid, address, length);
    	Artifact memoryArtifact = putArtifact(eventData, memoryInfo, true);
		
		Process process = putProcess(eventData); //create if doesn't exist
		
		WasGeneratedBy edge = new WasGeneratedBy(memoryArtifact, process);
		edge.addAnnotation("protection", protection);
		edge.addAnnotation("time", time);
		edge.addAnnotation("operation", getOperation(syscall));
		addEventIdAndSourceAnnotationToEdge(edge, eventData.get("eventid"), DEV_AUDIT);
		
		putEdge(edge);
    }

    private void handleKill(Map<String, String> eventData){
    	if(!CREATE_BEEP_UNITS){
    		return;
    	}
    	String pid = eventData.get("pid");
    	String time = eventData.get("time");
    	BigInteger arg0 = null;
    	BigInteger arg1 = null;
    	String unitId = null;
    	try{
    		arg0 = new BigInteger(eventData.get("a0"));
    		arg1 = new BigInteger(eventData.get("a1"));
    		unitId = arg1.toString();
    	}catch(Exception e){
    		logger.log(Level.WARNING, "Failed to process kill syscall", e);
    		return;
    	}
    	if(arg0.intValue() == -100){ //unit start
    		putProcess(eventData); //check if it exists. if not then add and return.
    		Process addedUnit = pushUnitIterationOnStack(pid, unitId, time); //create unit and add it to data structure
    		//add edge between the new unit and the main unit to keep the graph connected
        	putWasTriggeredByEdge(addedUnit, getContainingProcessVertex(pid), eventData.get("eventid"), time, BEEP, getOperation(SYSCALL.UNIT));
    	}else if(arg0.intValue() == -101){ //unit end
    		//remove all iterations of the given unit
    		popUnitIterationsFromStack(pid, unitId);
    	}else if(arg0.intValue() == -200 || arg0.intValue() == -300){ //-200 highbits of read, -300 highbits of write
    		pidToMemAddress.put(pid, arg1);
    	}else if(arg0.intValue() == -201 || arg0.intValue() == -301){ //-201 lowbits of read, -301 lowbits of write 
    		BigInteger address = pidToMemAddress.get(pid);
    		if(address != null){
    			Artifact memArtifact = null;
    			AbstractEdge edge = null;
    			Process process = getProcess(pid);
    			if(process == null || process.getAnnotation("unit").equals("0")){ //process cannot be null or cannot be the containing process which is doing the memory read or write in BEEP
    				logger.log(Level.INFO, "Unit vertex not found. Possibly missing unit creation 'kill' syscall. event id '"+eventData.get("eventid")+"'");
    				return;
    			}
    			address = address.shiftLeft(32);
    			address = address.add(arg1);
    			pidToMemAddress.remove(pid);
    			if(arg0.intValue() == -201){
    				memArtifact = putArtifact(eventData, new MemoryIdentity(pid, address.toString(16), ""), false, BEEP);
    				edge = new Used(process, memArtifact);
    				edge.addAnnotation("operation", getOperation(SYSCALL.READ));
    			}else if(arg0.intValue() == -301){
    				memArtifact = putArtifact(eventData, new MemoryIdentity(pid, address.toString(16), ""), true, BEEP);
    				edge = new WasGeneratedBy(memArtifact, process);
    				edge.addAnnotation("operation", getOperation(SYSCALL.WRITE));
    			}
    			if(edge != null && memArtifact != null && process != null){
	    			edge.addAnnotation("time", eventData.get("time"));
	    			addEventIdAndSourceAnnotationToEdge(edge, eventData.get("eventid"), BEEP);
	    			putEdge(edge);
    			}
    		}
    	}
    }

    private void handleForkClone(Map<String, String> eventData, SYSCALL syscall) {
        // fork() and clone() receive the following message(s):
        // - SYSCALL
        // - EOE

        String time = eventData.get("time");
        String oldPID = eventData.get("pid");
        String newPID = eventData.get("exit");
        
        if(syscall == SYSCALL.CLONE){
        	Long flags = CommonFunctions.parseLong(eventData.get("a0"), 0L);
        	//source: http://www.makelinux.net/books/lkd2/ch03lev1sec3
        	if((flags & SIGCHLD) == SIGCHLD && (flags & CLONE_VM) == CLONE_VM && (flags & CLONE_VFORK) == CLONE_VFORK){ //is vfork
        		syscall = SYSCALL.VFORK;
        	}else if((flags & SIGCHLD) == SIGCHLD){ //is fork
        		syscall = SYSCALL.FORK;
        	}
        	//otherwise it is just clone
        }        
        
        Process oldProcess = putProcess(eventData); //will create if doesn't exist

        // Whenever any new annotation is added to a Process which doesn't come from audit log then update the following ones. TODO
        Map<String, String> newEventData = new HashMap<String, String>();
        newEventData.putAll(eventData);
        newEventData.put("pid", newPID);
        newEventData.put("ppid", oldPID);
        newEventData.put("commandline", oldProcess.getAnnotation("commandline"));
        newEventData.put("cwd", eventData.get("cwd"));
        newEventData.put("start time", time);
        
        boolean RECREATE_AND_REPLACE = true; //true because a new process with the same pid might be being created. pids are recycled.
        Process newProcess = putProcess(newEventData, RECREATE_AND_REPLACE);
        
        putWasTriggeredByEdge(newProcess, oldProcess, eventData.get("eventid"), time, DEV_AUDIT, getOperation(syscall));
        
        if(syscall == SYSCALL.CLONE){
        	descriptors.linkDescriptors(oldPID, newPID);//share file descriptors when clone
        }else if(syscall == SYSCALL.FORK || syscall == SYSCALL.VFORK){ //copy file descriptors just once here when fork
        	descriptors.copyDescriptors(oldPID, newPID);
        }
    }

    private void handleExecve(Map<String, String> eventData) {
        // execve() receives the following message(s):
        // - SYSCALL
        // - EXECVE
        // - BPRM_FCAPS (ignored)
        // - CWD
        // - PATH
        // - PATH
        // - EOE

    	/*
    	 * Steps:
    	 * 0) check and get if the vertex with the pid exists already
    	 * 1) create the new vertex with the commandline which also replaces the vertex with the same pid
    	 * 2) if the vertex gotten in step 0 is null then it means that we missed that vertex
    	 * and cannot know what it uids and gids were. So, not trying to repair that vertex and not going
    	 * to put the edge from the child to the parent.
    	 * 3) add the Used edges from the newly created vertex in step 1 to the libraries used when execve
    	 *  was done
    	 */
    	
        String pid = eventData.get("pid");
        String time = eventData.get("time");

        String commandline = null;
        if(eventData.get("execve_argc") != null){
        	Long argc = CommonFunctions.parseLong(eventData.get("execve_argc"), 0L);
        	commandline = "";
        	for(int i = 0; i < argc; i++){
        		commandline += eventData.get("execve_a" + i) + " ";
        	}
        	commandline = commandline.trim();
        }else{
        	commandline = "[Record Missing]";
        }
        
        eventData.put("commandline", commandline);
        eventData.put("start time", time);
        
        //doing it before recreating and replacing the vertex with the same pid
        //try to get it. if doesn't exist then don't add it because it's user or group identifiers might have been different
        Process oldProcess = getProcess(pid); 
        
        boolean RECREATE_AND_REPLACE = true; //true because a process vertex with the same pid created in execve
        //this call would clear all the units for the pid because the process is doing execve, replacing itself.
        Process newProcess = putProcess(eventData, RECREATE_AND_REPLACE);
        
        if(oldProcess != null){
	        putWasTriggeredByEdge(newProcess, oldProcess, eventData.get("eventid"), time, DEV_AUDIT, getOperation(SYSCALL.EXECVE));
        }else{
        	logger.log(Level.INFO, "Unable to find the process that did the execve. Not creating the execve edge. event id '"+eventData.get("eventid")+"'");
        }
        
        //add used edge to the paths in the event data. get the number of paths using the 'items' key and then iterate
        String cwd = eventData.get("cwd");
        Long totalPaths = CommonFunctions.parseLong(eventData.get("items"), 0L);
        for(int pathNumber = 0; pathNumber < totalPaths; pathNumber++){
        	String path = eventData.get("path"+pathNumber);
        	path = constructAbsolutePathIfNotAbsolute(cwd, path);
        	if(path == null){
        		logger.log(Level.INFO, "Unable to create load edge for execve syscall. event id '"+eventData.get("eventid")+"'");
        		continue;
        	}        	
        	ArtifactIdentity fileIdentity = new FileIdentity(path);
        	Artifact usedArtifact = putArtifact(eventData, fileIdentity, false);
        	Used usedEdge = new Used(newProcess, usedArtifact);
        	usedEdge.addAnnotation("time", time);
        	usedEdge.addAnnotation("operation", getOperation(SYSCALL.LOAD));
        	addEventIdAndSourceAnnotationToEdge(usedEdge, eventData.get("eventid"), DEV_AUDIT);
        	putEdge(usedEdge);
        }
        
        descriptors.unlinkDescriptors(pid);
    }
    
    private void handleCreat(Map<String, String> eventData){
    	//creat() receives the following message(s):
    	// - SYSCALL
        // - CWD
        // - PATH of the parent with nametype=PARENT
        // - PATH of the created file with nametype=CREATE
        // - EOE
    	
    	//as mentioned in open syscall manpage    	
    	int defaultFlags = O_CREAT|O_WRONLY|O_TRUNC;
    	
    	//modify the eventData as expected by open syscall and call open syscall function
    	eventData.put("a2", eventData.get("a1")); //set mode to argument 3 (in open) from 2 (in creat)
    	eventData.put("a1", String.valueOf(defaultFlags)); //convert defaultflags argument to hexadecimal
    	
    	handleOpen(eventData, SYSCALL.CREATE); //TODO change to creat. kept as create to keep current CDM data consistent
    	
    }
    
    private void handleOpenat(Map<String, String> eventData){
    	//openat() receives the following message(s):
    	// - SYSCALL
        // - CWD
        // - PATH
        // - PATH
        // - EOE
    	
    	Long dirFd = CommonFunctions.parseLong(eventData.get("a0"), -1L);
    	
    	//according to manpage if following true then use cwd if path not absolute, which is already handled by open
    	if(dirFd != AT_FDCWD){ //checking if cwd needs to be replaced by dirFd's path
    		String pid = eventData.get("pid");
			String dirFdString = String.valueOf(dirFd);
			//if null of if not file then cannot process it
			ArtifactIdentity artifactIdentity = descriptors.getDescriptor(pid, dirFdString);
			if(artifactIdentity == null || !FileIdentity.class.equals(artifactIdentity.getClass())){
				logger.log(Level.INFO, "openat doesn't support directory fds of type={0}. event id {1}", new Object[]{String.valueOf(artifactIdentity), String.valueOf(eventData.get("eventid"))});
				return;
			}else{ //is file
				String dirPath = ((FileIdentity) artifactIdentity).getPath();
				eventData.put("cwd", dirPath); //replace cwd with dirPath to make eventData compatible with open
			}
    	}
    	
    	//modify the eventData to match open syscall and then call it's function
    	
    	eventData.put("a0", eventData.get("a1")); //moved pathname address to first like in open
    	eventData.put("a1", eventData.get("a2")); //moved flags to second like in open
    	eventData.put("a2", eventData.get("a3")); //moved mode to third like in open
    	
    	handleOpen(eventData, SYSCALL.OPENAT);
    }

    private void handleOpen(Map<String, String> eventData, SYSCALL syscall) {
        // open() receives the following message(s):
        // - SYSCALL
        // - CWD
    	// - PATH with nametype CREATE (file operated on) or NORMAL (file operated on) or PARENT (parent of file operated on) or DELETE (file operated on) or UNKNOWN (only when syscall fails)
    	// - PATH with nametype CREATE or NORMAL or PARENT or DELETE or UNKNOWN
        // - EOE
    	
    	//three syscalls can come here: OPEN (for files and pipes), OPENAT (for files and pipes), CREAT (only for files)
    	    	
    	Long flags = CommonFunctions.parseLong(eventData.get("a1"), 0L);
    	
        String pid = eventData.get("pid");
        String cwd = eventData.get("cwd");
        boolean isCreate = syscall == SYSCALL.CREATE || syscall == SYSCALL.CREAT; //TODO later on change only to CREAT only
        String path = null;
        Map<Integer, String> paths = getPathsWithNametype(eventData, "CREATE"); 
        if(paths.size() == 0){
        	isCreate = false;
        	paths = getPathsWithNametype(eventData, "NORMAL");
        	if(paths.size() == 0){ //missing audit record
        		logger.log(Level.INFO, "Missing required path record in 'open'. event id '"+eventData.get("eventid")+"'");
        		return;
        	}
        }else{ //found path with CREATE nametype. will always be one
        	isCreate = true;        	
        }
        path = paths.values().iterator().next(); //get the first
        String fd = eventData.get("exit");
        String time = eventData.get("time");
        
        path = constructAbsolutePathIfNotAbsolute(cwd, path);
        
        if(path == null){
        	logger.log(Level.WARNING, "Missing CWD or PATH record in 'open'. Event with id '"+ eventData.get("eventid"));
        	return;
        }
        
        Process process = putProcess(eventData);
        
        ArtifactIdentity artifactIdentity = getValidArtifactIdentityForPath(path);
        
        AbstractEdge edge = null;
        
        if(isCreate){
        	
        	if(!FileIdentity.class.equals(artifactIdentity.getClass())){
        		artifactIdentity = new FileIdentity(path); //can only create a file using open
        	}
        	
        	//set new epoch
        	getArtifactProperties(artifactIdentity).markNewEpoch(eventData.get("eventid"));
        	
        	Artifact vertex = putArtifact(eventData, artifactIdentity, true); //updating version too
            edge = new WasGeneratedBy(vertex, process);
            
            syscall = SYSCALL.CREATE;
            
        }else{
        	
        	if((!FileIdentity.class.equals(artifactIdentity.getClass()) && !artifactIdentity.getClass().equals(NamedPipeIdentity.class))){ //not a file and not a named pipe
        		//make it a file identity
        		artifactIdentity = new FileIdentity(path);
        	}
        
        	if ((flags & O_RDONLY) == O_RDONLY) {
        		Artifact vertex = putArtifact(eventData, artifactIdentity, false);
	            edge = new Used(process, vertex);
        	} else if((flags & O_WRONLY) == O_WRONLY || (flags & O_RDWR) == O_RDWR){
        		Artifact vertex = putArtifact(eventData, artifactIdentity, true);
 	            edge = new WasGeneratedBy(vertex, process);
	        } else{
	        	logger.log(Level.INFO, "Unknown flag for open '"+flags+"'. event id '"+eventData.get("eventid")+"'" );
	        	return;
	        }
	        
        }
        if(edge != null){
        	//everything happened successfully. add it to descriptors
        	descriptors.addDescriptor(pid, fd, artifactIdentity);
        	
        	//put the edge
	        edge.addAnnotation("operation", getOperation(syscall));
	        edge.addAnnotation("time", time);
	        addEventIdAndSourceAnnotationToEdge(edge, eventData.get("eventid"), DEV_AUDIT);
	        putEdge(edge);
        }
    }

    private void handleClose(Map<String, String> eventData) {
        // close() receives the following message(s):
        // - SYSCALL
        // - EOE
        
    	String pid = eventData.get("pid");
        String fd = String.valueOf(CommonFunctions.parseLong(eventData.get("a0"), -1L));
        descriptors.removeDescriptor(pid, fd);
       
        //there is an option to either handle epochs 1) when artifact opened/created or 2) when artifacts deleted/closed.
        //handling epoch at opened/created in all cases
    }

    private void handleRead(Map<String, String> eventData, SYSCALL syscall) {
        // read() receives the following message(s):
        // - SYSCALL
        // - EOE
        String pid = eventData.get("pid");
        String fd = eventData.get("a0");
        String bytesRead = eventData.get("exit");
        Process process = putProcess(eventData);

        String time = eventData.get("time");
        
        if(descriptors.getDescriptor(pid, fd) == null){
        	descriptors.addUnknownDescriptor(pid, fd);
        	getArtifactProperties(descriptors.getDescriptor(pid, fd)).markNewEpoch(eventData.get("eventid"));
        }
        
        ArtifactIdentity artifactIdentity = descriptors.getDescriptor(pid, fd);
        Artifact vertex = putArtifact(eventData, artifactIdentity, false);
        Used used = new Used(process, vertex);
        used.addAnnotation("operation", getOperation(syscall));
        used.addAnnotation("time", time);
        used.addAnnotation("size", bytesRead);
        addEventIdAndSourceAnnotationToEdge(used, eventData.get("eventid"), DEV_AUDIT);
        putEdge(used);
        
    }

    private void handleWrite(Map<String, String> eventData, SYSCALL syscall) {
        // write() receives the following message(s):
        // - SYSCALL
        // - EOE
        String pid = eventData.get("pid");
        Process process = putProcess(eventData);

        String fd = eventData.get("a0");
        String time = eventData.get("time");
        String bytesWritten = eventData.get("exit");
        
        if(descriptors.getDescriptor(pid, fd) == null){
        	descriptors.addUnknownDescriptor(pid, fd);
        	getArtifactProperties(descriptors.getDescriptor(pid, fd)).markNewEpoch(eventData.get("eventid"));
        }
        
        ArtifactIdentity artifactIdentity = descriptors.getDescriptor(pid, fd);
        
        Artifact vertex = putArtifact(eventData, artifactIdentity, true);
        WasGeneratedBy wgb = new WasGeneratedBy(vertex, process);
        wgb.addAnnotation("operation", getOperation(syscall));
        wgb.addAnnotation("time", time);
        wgb.addAnnotation("size", bytesWritten);
        addEventIdAndSourceAnnotationToEdge(wgb, eventData.get("eventid"), DEV_AUDIT);
        putEdge(wgb);
    }

    private void handleTruncate(Map<String, String> eventData, SYSCALL syscall) {
        // write() receives the following message(s):
        // - SYSCALL
        // - EOE
        String pid = eventData.get("pid");
        Process process = putProcess(eventData);

        String time = eventData.get("time");
        ArtifactIdentity artifactIdentity = null;

        if (syscall == SYSCALL.TRUNCATE) {
        	Map<Integer, String> paths = getPathsWithNametype(eventData, "NORMAL");
        	if(paths.size() == 0){
        		logger.log(Level.INFO, "Missing required path in 'truncate'. event id '"+eventData.get("eventid")+"'");
        		return;
        	}
        	String path = paths.values().iterator().next();
        	path = constructAbsolutePathIfNotAbsolute(eventData.get("cwd"), path);
        	if(path == null){
        		logger.log(Level.INFO, "Missing required CWD record in 'truncate'. event id '"+eventData.get("eventid")+"'");
        		return;
        	}
            artifactIdentity = new FileIdentity(path);
        } else if (syscall == SYSCALL.FTRUNCATE) {
        	String fd = eventData.get("a0");
            
            if(descriptors.getDescriptor(pid, fd) == null){
            	descriptors.addUnknownDescriptor(pid, fd);
            	getArtifactProperties(descriptors.getDescriptor(pid, fd)).markNewEpoch(eventData.get("eventid"));
            }
                        
            artifactIdentity = descriptors.getDescriptor(pid, fd);
        }

        if(FileIdentity.class.equals(artifactIdentity.getClass()) || UnknownIdentity.class.equals(artifactIdentity.getClass())){
        	Artifact vertex = putArtifact(eventData, artifactIdentity, true);
            WasGeneratedBy wgb = new WasGeneratedBy(vertex, process);
            wgb.addAnnotation("operation", getOperation(syscall));
            wgb.addAnnotation("time", time);
            addEventIdAndSourceAnnotationToEdge(wgb, eventData.get("eventid"), DEV_AUDIT);
            putEdge(wgb);
        }else{
        	logger.log(Level.INFO, "Unexpected artifact type '"+artifactIdentity+"' for truncate. event id '"+eventData.get("eventid")+"'");
        }  
    }

    private void handleDup(Map<String, String> eventData, SYSCALL syscall) {
        // dup(), dup2(), and dup3() receive the following message(s):
        // - SYSCALL
        // - EOE
        String pid = eventData.get("pid");

        String fd = eventData.get("a0");
        String newFD = eventData.get("exit"); //new fd returned in all: dup, dup2, dup3
        
        if(!fd.equals(newFD)){ //if both fds same then it succeeds in case of dup2 and it does nothing so do nothing here too
            if(descriptors.getDescriptor(pid, fd) == null){
	        	descriptors.addUnknownDescriptor(pid, fd);
	        	getArtifactProperties(descriptors.getDescriptor(pid, fd)).markNewEpoch(eventData.get("eventid"));
	        }
	        descriptors.addDescriptor(pid, newFD, descriptors.getDescriptor(pid, fd));
	        
        }
    }

    private void handleSetuid(Map<String, String> eventData, SYSCALL syscall) {
        // setuid() receives the following message(s):
        // - SYSCALL
        // - EOE
    	
    	String time = eventData.get("time");
        String pid = eventData.get("pid");
        String eventId = eventData.get("eventid");
        
        /*
         * Pseudo-code
         * 
         * oldProcess = current process with pid
         * if oldProcess is null then
         * 		putVertex(eventData)
         * 		put no edge since we don't have oldProcess
         * else
         * 		if oldProcess is not an iteration i.e. a simple process then
         * 			newProcess = putVertex(eventData)
         * 			draw edge from newProcess to oldProcess
         * 		else if oldProcess is an iteration then
         * 			oldContainingProcess = get old containing process
         * 			newContainingProcess = create new containing process
         * 			oldRunningIterationsList = list of all currently valid unit iterations (can be multiple when nested loop only)
         * 			newRunningIterationsList = copy of all unit iterations with updated fields
         * 			put newContainingVertex and all newRunningIterationsList to storage
         * 			create edge from newContainingProcess to oldContainingProcess
         * 			for each newRunningIteration in newRunningIterationsList
         * 				draw edge from newRunningIteration to oldRunningIteration
         * 				draw edge from newRunningIteration to newContainingProcess
         * 			create edge from newProcessIteration to oldProcess (which is a unit iteration)
         * 			//now update internal data structures	
         * 			manually replace oldContainingProcess with newContainingProcess
         * 			
         * 			manually add newProcessIteration to the process stack. Doing this manually so as to not reset the iteration counter for the process
         * 	
         */
        
        Process exitingVertex = getProcess(pid);
        
        if(exitingVertex == null){
        	putProcess(eventData); //can't add the edge since no existing vertex with the same pid
        }else{
        	
        	// Following are the annotations that need to be updated in processes and units while keep all other the same
        	Map<String, String> annotationsToUpdate = new HashMap<String, String>();
    		annotationsToUpdate.put("auid", eventData.get("auid"));
    		annotationsToUpdate.put("uid", eventData.get("uid"));
    		annotationsToUpdate.put("suid", eventData.get("suid"));
    		annotationsToUpdate.put("euid", eventData.get("euid"));
    		annotationsToUpdate.put("fsuid", eventData.get("fsuid"));
        	
        	/* A check for containing process. Either no unit annotation meaning that units weren't enabled or
        	 * the units are enabled and the unit annotation value is zero i.e. the containing process	
        	 */  
        	if(exitingVertex.getAnnotation("unit") == null || exitingVertex.getAnnotation("unit").equals("0")){
        		Map<String, String> existingProcessAnnotations = exitingVertex.getAnnotations();
        		Map<String, String> newProcessAnnotations = updateKeyValuesInMap(existingProcessAnnotations, annotationsToUpdate);
        		//here then it means that there are no active units for the process
        		//update the modified annotations by the syscall and create the new process vertex
        		boolean RECREATE_AND_REPLACE = true; //has to be true since already an entry for the same pid exists
        		Process newProcess = putProcess(newProcessAnnotations, RECREATE_AND_REPLACE); 
        		//drawing edge from the new to the old
        		putWasTriggeredByEdge(newProcess, exitingVertex, eventId, time, DEV_AUDIT, getOperation(syscall));
        	}else{ //is a unit i.e. unit annotation is non-null and non-zero. 
        		
        		// oldProcessUnitStack has only active iterations. Should be only one active one since nested loops haven't been instrumented yet in BEEP.
        		// but taking care of nested loops still anyway. 
        		// IMPORTANT: Getting this here first because putProcess call on newContainingProcess would discard it
        		LinkedList<Process> oldProcessUnitStack = processUnitStack.get(pid);
        		
        		Process oldContainingProcess = getContainingProcessVertex(pid);
        		Map<String, String> oldContainingProcessAnnotations = oldContainingProcess.getAnnotations();
        		Map<String, String> newContainingProcessAnnotations = updateKeyValuesInMap(oldContainingProcessAnnotations, annotationsToUpdate);
        		boolean RECREATE_AND_REPLACE = true; //has to be true since already an entry for the same pid exists
        		Process newContainingProcess = putProcess(newContainingProcessAnnotations, RECREATE_AND_REPLACE);
        		
        		//Get the new process unit stack now in which the new units would be added. New because putProcess above has replaced the old one
        		LinkedList<Process> newProcessUnitStack = processUnitStack.get(pid);
        		
        		putWasTriggeredByEdge(newContainingProcess, oldContainingProcess, eventId, time, DEV_AUDIT, getOperation(syscall));
        		
        		//recreating the rest of the stack manually because existing iteration and count annotations need to be preserved
        		for(int a = 1; a<oldProcessUnitStack.size(); a++){ //start from 1 because 0 is the containing process
        			Process oldProcessUnit = oldProcessUnitStack.get(a);
        			Map<String, String> oldProcessUnitAnnotations = oldProcessUnit.getAnnotations();
        			Map<String, String> newProcessUnitAnnotations = updateKeyValuesInMap(oldProcessUnitAnnotations, annotationsToUpdate);
        			Process newProcessUnit = createProcessVertex(newProcessUnitAnnotations); //create process unit
        			newProcessUnitStack.addLast(newProcessUnit); //add to memory
        			putVertex(newProcessUnit); //add to buffer
        			
        			//drawing an edge from newProcessUnit to currentProcessUnit with operation based on current syscall
        			putWasTriggeredByEdge(newProcessUnit, oldProcessUnit, eventId, time, DEV_AUDIT, getOperation(syscall));
        			//drawing an edge from newProcessUnit to newContainingProcess with operation unit to keep things consistent
        			putWasTriggeredByEdge(newProcessUnit, newContainingProcess, eventId, time, BEEP, getOperation(SYSCALL.UNIT));
        		}
        	}
        }
    }

    private void handleRename(Map<String, String> eventData) {
        // rename() receives the following message(s):
        // - SYSCALL
        // - CWD
        // - PATH 0 is directory of <src>
        // - PATH 1 is directory of <dst>
        // - PATH 2 is path of <src> relative to <cwd>
        // - PATH 3 is path of <dst> relative to <cwd>
        // - EOE
        // we use cwd and paths 2 and 3
        String time = eventData.get("time");
        String pid = eventData.get("pid");
//        String cwd = eventData.get("cwd");
        
        String srcDirPath = eventData.get("path0");
        String dstDirPath = eventData.get("path1");
        
        Process process = putProcess(eventData);

        String srcpath = constructAbsolutePathIfNotAbsolute(srcDirPath, eventData.get("path2"));
        String dstpath = constructAbsolutePathIfNotAbsolute(dstDirPath, eventData.get("path3"));
                
        if(srcpath == null || dstpath == null){
        	logger.log(Level.INFO, "Missing required PATH or CWD records in 'rename'. event id '"+eventData.get("eventid")+"'");
        	return;
        }
        
        ArtifactIdentity srcArtifactIdentity = getValidArtifactIdentityForPath(srcpath);
        ArtifactIdentity dstArtifactIdentity = null;
        
        if(FileIdentity.class.equals(srcArtifactIdentity.getClass())){
        	dstArtifactIdentity = new FileIdentity(dstpath);
        }else if(NamedPipeIdentity.class.equals(srcArtifactIdentity.getClass())){
        	dstArtifactIdentity = new NamedPipeIdentity(dstpath);
        }else if(UnixSocketIdentity.class.equals(srcArtifactIdentity.getClass())){
        	dstArtifactIdentity = new UnixSocketIdentity(dstpath);
        }else{
        	logger.log(Level.INFO, "Unexpected artifact type '"+srcArtifactIdentity+"' for rename. event id '"+eventData.get("eventid")+"'");
        	return;
        }

        //destination is always created. So, set the epoch whenever later on it is opened
        getArtifactProperties(dstArtifactIdentity).markNewEpoch(eventData.get("eventid"));
        
        Artifact srcVertex = putArtifact(eventData, srcArtifactIdentity, false);
        Used used = new Used(process, srcVertex);
        used.addAnnotation("operation", getOperation(SYSCALL.RENAME)+"_"+getOperation(SYSCALL.READ));
        used.addAnnotation("time", time);
        addEventIdAndSourceAnnotationToEdge(used, eventData.get("eventid"), DEV_AUDIT);
        putEdge(used);

        Artifact dstVertex = putArtifact(eventData, dstArtifactIdentity, true);
        WasGeneratedBy wgb = new WasGeneratedBy(dstVertex, process);
        wgb.addAnnotation("operation", getOperation(SYSCALL.RENAME)+"_"+getOperation(SYSCALL.WRITE));
        wgb.addAnnotation("time", time);
        addEventIdAndSourceAnnotationToEdge(wgb, eventData.get("eventid"), DEV_AUDIT);
        putEdge(wgb);

        WasDerivedFrom wdf = new WasDerivedFrom(dstVertex, srcVertex);
        wdf.addAnnotation("operation", getOperation(SYSCALL.RENAME));
        wdf.addAnnotation("time", time);
        wdf.addAnnotation("pid", pid);
        addEventIdAndSourceAnnotationToEdge(wdf, eventData.get("eventid"), DEV_AUDIT);
        putEdge(wdf);
    }
    
    private void handleMknodat(Map<String, String> eventData){
    	//mknodat() receives the following message(s):
    	// - SYSCALL
        // - CWD
        // - PATH of the created file with nametype=CREATE
        // - EOE
    	
    	//first argument is the fd of the directory to create file in. if the directory fd is AT_FDCWD then use cwd
    	
    	String pid = eventData.get("pid");
    	String fd = eventData.get("a0");
    	Long fdLong = CommonFunctions.parseLong(fd, null);
    	
    	ArtifactIdentity artifactIdentity = null;
    	
    	if(fdLong != AT_FDCWD){
    		artifactIdentity = descriptors.getDescriptor(pid, fd);
    		if(artifactIdentity == null){
    			logger.log(Level.INFO, "Couldn't find directory fd in local map for mknodat. event id = '"+eventData.get("eventid")+"'");
    			return;
    		}else if(artifactIdentity.getClass().equals(FileIdentity.class)){
    			String directoryPath = ((FileIdentity)artifactIdentity).getPath();
	    		//update cwd to directoryPath and call handleMknod. the file created path is always relative in this syscall
	    		eventData.put("cwd", directoryPath);
    		}else{
    			logger.log(Level.INFO, "Couldn't find directory fd in local map for 'mknodat'. event id = '"+eventData.get("eventid")+"' " + " artifact type = " + artifactIdentity.getClass());
    			return;
    		}    		
    	}
   	
    	//replace the second argument (which is mode in mknod) with the third (which is mode in mknodat)
		eventData.put("a1", eventData.get("a2"));
		handleMknod(eventData, SYSCALL.MKNODAT);
    }
    
    private void handleMknod(Map<String, String> eventData, SYSCALL syscall){
    	//mknod() receives the following message(s):
    	// - SYSCALL
        // - CWD
        // - PATH of the parent with nametype=PARENT
        // - PATH of the created file with nametype=CREATE
        // - EOE
    	
    	String modeString = eventData.get("a1");
    	
    	Long mode = CommonFunctions.parseLong(modeString, 0L);
    	
    	String parentPath = null;
    	if(syscall == SYSCALL.MKNODAT){
    		parentPath = eventData.get("cwd");
    	}else if(syscall == SYSCALL.MKNOD){
    		
    		Map<Integer, String> parentPaths = getPathsWithNametype(eventData, "PARENT");
    		if(parentPaths.size() != 0){
    			parentPath = parentPaths.values().iterator().next();
    		}
    		
    	}else{
    		logger.log(Level.INFO, "Unexpected syscall '"+syscall+"' in mknod. event id '"+eventData.get("eventid")+"'");
    		return;
    	}
    	        
        String path = null;
        
        Map<Integer, String> paths = getPathsWithNametype(eventData, "CREATE");
                
        path = paths.values().iterator().next();        
        path = constructAbsolutePathIfNotAbsolute(parentPath, path);
        
        if(path == null){
        	logger.log(Level.INFO, "Missing records for syscall {0}. event id {1}", new Object[]{syscall, eventData.get("eventid")});
        	return;
        }
        
        ArtifactIdentity artifactIdentity = null;
    	
    	if((mode & S_IFIFO) == S_IFIFO){ //is pipe
            artifactIdentity = new NamedPipeIdentity(path);
    	}else if((mode & S_IFREG) == S_IFREG){ //is regular file
    		artifactIdentity = new FileIdentity(path);
    	}else if((mode & S_IFSOCK) == S_IFSOCK){ //is unix socket
    		artifactIdentity = new UnixSocketIdentity(path);
    	}else{
    		logger.log(Level.INFO, "Unsupported mode for mknod '"+mode+"'. event id '"+eventData.get("eventid")+"'");
    		return;
    	}	
    	
    	if(artifactIdentity != null){
	    	getArtifactProperties(artifactIdentity).markNewEpoch(eventData.get("eventid"));
    	}
    }

    private void handleLink(Map<String, String> eventData, SYSCALL syscall) {
        // link() and symlink() receive the following message(s):
        // - SYSCALL
        // - CWD
        // - PATH 0 is path of <src> relative to <cwd>
        // - PATH 1 is directory of <dst>
        // - PATH 2 is path of <dst> relative to <cwd>
        // - EOE
        // we use cwd and paths 0 and 2
    	
        String time = eventData.get("time");
        String pid = eventData.get("pid");
        String cwd = eventData.get("cwd");
        Process process = putProcess(eventData);
        
        String syscallName = getOperation(syscall);

        String srcpath = constructAbsolutePathIfNotAbsolute(cwd, eventData.get("path0"));
        String dstpath = constructAbsolutePathIfNotAbsolute(eventData.get("path1"), eventData.get("path2"));
        
        if(srcpath == null || dstpath == null){
        	logger.log(Level.INFO, "Missing CWD or PATH records in 'link' syscall. event id '"+eventData.get("eventid")+"'");
        	return;
        }
        
        ArtifactIdentity srcArtifactIdentity = getValidArtifactIdentityForPath(srcpath);
        ArtifactIdentity dstArtifactIdentity = null;
        
        if(FileIdentity.class.equals(srcArtifactIdentity.getClass())){
        	dstArtifactIdentity = new FileIdentity(dstpath);
        }else if(NamedPipeIdentity.class.equals(srcArtifactIdentity.getClass())){
        	dstArtifactIdentity = new NamedPipeIdentity(dstpath);
        }else if(UnixSocketIdentity.class.equals(srcArtifactIdentity.getClass())){
        	dstArtifactIdentity = new UnixSocketIdentity(dstpath);
        }else{
        	logger.log(Level.INFO, "Unexpected artifact type '"+srcArtifactIdentity+"' for link. event id '"+eventData.get("eventid")+"'");
        	return;
        }
        
        //destination is new so mark epoch
        getArtifactProperties(dstArtifactIdentity).markNewEpoch(eventData.get("eventid"));

        Artifact srcVertex = putArtifact(eventData, srcArtifactIdentity, false);
        Used used = new Used(process, srcVertex);
        used.addAnnotation("operation", syscallName + "_" + getOperation(SYSCALL.READ));
        used.addAnnotation("time", time);
        addEventIdAndSourceAnnotationToEdge(used, eventData.get("eventid"), DEV_AUDIT);
        putEdge(used);

        Artifact dstVertex = putArtifact(eventData, dstArtifactIdentity, true);
        WasGeneratedBy wgb = new WasGeneratedBy(dstVertex, process);
        wgb.addAnnotation("operation", syscallName + "_" + getOperation(SYSCALL.WRITE));
        wgb.addAnnotation("time", time);
        addEventIdAndSourceAnnotationToEdge(wgb, eventData.get("eventid"), DEV_AUDIT);
        putEdge(wgb);

        WasDerivedFrom wdf = new WasDerivedFrom(dstVertex, srcVertex);
        wdf.addAnnotation("operation", syscallName);
        wdf.addAnnotation("time", time);
        wdf.addAnnotation("pid", pid);
        addEventIdAndSourceAnnotationToEdge(wdf, eventData.get("eventid"), DEV_AUDIT);
        putEdge(wdf);
    }

    private void handleChmod(Map<String, String> eventData, SYSCALL syscall) {
        // chmod() receives the following message(s):
        // - SYSCALL
        // - CWD
        // - PATH
        // - EOE
        String time = eventData.get("time");
        String pid = eventData.get("pid");
        Process process = putProcess(eventData);
        // mode is in hex format in <a1>
        String mode = new BigInteger(eventData.get("a1")).toString(8);
        // if syscall is chmod, then path is <path0> relative to <cwd>
        // if syscall is fchmod, look up file descriptor which is <a0>
        ArtifactIdentity artifactIdentity = null;
        if (syscall == SYSCALL.CHMOD) {
        	Map<Integer, String> paths = getPathsWithNametype(eventData, "NORMAL");
        	if(paths.size() == 0){
        		logger.log(Level.INFO, "Missing required path in 'chmod'. event id '"+eventData.get("eventid")+"'");
        		return;
        	}
        	String path = paths.values().iterator().next();
        	path = constructAbsolutePathIfNotAbsolute(eventData.get("cwd"), path);
        	if(path == null){
        		logger.log(Level.INFO, "Missing required CWD record in 'chmod'. event id '"+eventData.get("eventid")+"'");
        		return;
        	}
            artifactIdentity = getValidArtifactIdentityForPath(path);
        } else if (syscall == SYSCALL.FCHMOD) {
        	
        	String fd = eventData.get("a0");
            
            if(descriptors.getDescriptor(pid, fd) == null){
            	descriptors.addUnknownDescriptor(pid, fd);
            	getArtifactProperties(descriptors.getDescriptor(pid, fd)).markNewEpoch(eventData.get("eventid"));
            }
                        
            artifactIdentity = descriptors.getDescriptor(pid, fd);
        }
        
        Artifact vertex = putArtifact(eventData, artifactIdentity, true);
        WasGeneratedBy wgb = new WasGeneratedBy(vertex, process);
        wgb.addAnnotation("operation", getOperation(syscall));
        wgb.addAnnotation("mode", mode);
        wgb.addAnnotation("time", time);
        addEventIdAndSourceAnnotationToEdge(wgb, eventData.get("eventid"), DEV_AUDIT);
        putEdge(wgb);
    }

    private void handlePipe(Map<String, String> eventData, SYSCALL syscall) {
    	// pipe() receives the following message(s):
        // - SYSCALL
        // - FD_PAIR
        // - EOE
        String pid = eventData.get("pid");

        String fd0 = eventData.get("fd0");
        String fd1 = eventData.get("fd1");
        ArtifactIdentity pipeInfo = new UnnamedPipeIdentity(pid, fd0, fd1);
        descriptors.addDescriptor(pid, fd0, pipeInfo);
        descriptors.addDescriptor(pid, fd1, pipeInfo);
        
        getArtifactProperties(pipeInfo).markNewEpoch(eventData.get("eventid"));
    }    
    
    
    /*private void handleNetfilterPacketEvent(Map<String, String> eventData){
      Refer to the following link for protocol numbers
//    	http://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
    	String protocol = eventData.get("proto");
    	
    	if(protocol.equals("6") || protocol.equals("17")){ // 6 is tcp and 17 is udp
    		String length = eventData.get("len");
        	
        	hook = 1 is input, hook = 3 is forward
        	String hook = eventData.get("hook");
        	
        	String sourceAddress = eventData.get("saddr");
        	String destinationAddress = eventData.get("daddr");
        	
        	String sourcePort = eventData.get("sport");
        	String destinationPort = eventData.get("dport");
        	
        	String time = eventData.get("time");
        	String eventId = eventData.get("eventid");
        	
        	SocketInfo source = new SocketInfo(sourceAddress, sourcePort);
        	SocketInfo destination = new SocketInfo(destinationAddress, destinationPort);
    	}
    	
    }*/
    
    private void handleBind(Map<String, String> eventData, SYSCALL syscall) {
    	// bind() receives the following message(s):
        // - SYSCALL
        // - SADDR
        // - EOE
        String saddr = eventData.get("saddr");
        ArtifactIdentity artifactIdentity = parseSaddr(saddr, syscall);
        if (artifactIdentity != null) {
        	if(UnixSocketIdentity.class.equals(artifactIdentity.getClass()) && !UNIX_SOCKETS){
        		return;
        	}
            String pid = eventData.get("pid");
            //NOTE: not using the file descriptor. using the socketFD here!
            //Doing this to be able to link the accept syscalls to the correct artifactIdentity.
            //In case of unix socket accept, the saddr is almost always reliably invalid
            String socketFd = eventData.get("a0");
            descriptors.addDescriptor(pid, socketFd, artifactIdentity);
        }else{
        	logger.log(Level.INFO, "Invalid saddr '"+saddr+"' in 'bind'. event id '"+eventData.get("eventid")+"'");
        }
    }
    
    private void handleConnect(Map<String, String> eventData) {
    	//connect() receives the following message(s):
        // - SYSCALL
        // - SADDR
        // - EOE
        String time = eventData.get("time");
        String pid = eventData.get("pid");
        String saddr = eventData.get("saddr");
        ArtifactIdentity parsedArtifactIdentity = parseSaddr(saddr, SYSCALL.CONNECT);
        if (parsedArtifactIdentity != null) {
        	if(UnixSocketIdentity.class.equals(parsedArtifactIdentity.getClass()) && !UNIX_SOCKETS){
        		return;
        	}
        	Process process = putProcess(eventData);
        	// update file descriptor table
            String fd = eventData.get("a0");
            descriptors.addDescriptor(pid, fd, parsedArtifactIdentity);
            getArtifactProperties(parsedArtifactIdentity).markNewEpoch(eventData.get("eventid"));
        	
            Artifact artifact = putArtifact(eventData, parsedArtifactIdentity, false);
            WasGeneratedBy wgb = new WasGeneratedBy(artifact, process);
            wgb.addAnnotation("time", time);
            wgb.addAnnotation("operation", getOperation(SYSCALL.CONNECT));
            addEventIdAndSourceAnnotationToEdge(wgb, eventData.get("eventid"), DEV_AUDIT);
            putEdge(wgb);
        }else{
        	logger.log(Level.INFO, "Unable to find artifact type from saddr field in 'connect'. event id '"+eventData.get("eventid")+"'");
        }
    }

    private void handleAccept(Map<String, String> eventData, SYSCALL syscall) {
    	//accept() & accept4() receive the following message(s):
        // - SYSCALL
        // - SADDR
        // - EOE
        String time = eventData.get("time");
        String pid = eventData.get("pid");
        String socketFd = eventData.get("a0"); //the fd on which the connection was accepted, not the fd of the connection
        String fd = eventData.get("exit"); //fd of the connection
        String saddr = eventData.get("saddr");
                
        ArtifactIdentity boundArtifactIdentity = descriptors.getDescriptor(pid, socketFd); //previously bound
        ArtifactIdentity parsedArtifactIdentity = parseSaddr(saddr, syscall);
        
        //discarding cases that cannot be handled
        if(parsedArtifactIdentity == null){ //if null then cannot do anything unless bound artifact was unix socket
        	if(boundArtifactIdentity == null || !UnixSocketIdentity.class.equals(boundArtifactIdentity.getClass())){
        		logger.log(Level.INFO, "Invalid or no 'saddr' in 'accept' syscall. event id '"+eventData.get("eventid")+"'");
        		return;
        	}else{ //is a unix socket identity
        		if(UnixSocketIdentity.class.equals(boundArtifactIdentity.getClass()) && !UNIX_SOCKETS){
            		return;
            	}
        		descriptors.addDescriptor(pid, fd, boundArtifactIdentity);
        	}
        }else if(UnixSocketIdentity.class.equals(parsedArtifactIdentity.getClass())){
        	if(UnixSocketIdentity.class.equals(parsedArtifactIdentity.getClass()) && !UNIX_SOCKETS){
        		return;
        	}
        	if(boundArtifactIdentity == null || !UnixSocketIdentity.class.equals(boundArtifactIdentity.getClass())){ //we need the bound address always
        		logger.log(Level.INFO, "Invalid or no 'saddr' in 'accept' syscall. event id '"+eventData.get("eventid")+"'");
        		return;
        	}else{ //is a unix socket identity
        		descriptors.addDescriptor(pid, fd, boundArtifactIdentity);
        	}
        }else if(NetworkSocketIdentity.class.equals(parsedArtifactIdentity.getClass())){
        	//anything goes. dont really need the bound. but if it is there then its good
        	if(boundArtifactIdentity == null || !NetworkSocketIdentity.class.equals(boundArtifactIdentity.getClass())){
        		descriptors.addDescriptor(pid, socketFd, new NetworkSocketIdentity("", "", "", "", "")); //add a dummy one if null or a mismatch
        	}
        	NetworkSocketIdentity boundSocketIdentity = (NetworkSocketIdentity)descriptors.getDescriptor(pid, socketFd);
        	NetworkSocketIdentity parsedSocketIdentity = (NetworkSocketIdentity)parsedArtifactIdentity;
        	ArtifactIdentity socketIdentity = new NetworkSocketIdentity(parsedSocketIdentity.getSourceHost(), parsedSocketIdentity.getSourcePort(),
        			boundSocketIdentity.getDestinationHost(), boundSocketIdentity.getDestinationPort(), parsedSocketIdentity.getProtocol());
        	descriptors.addDescriptor(pid, fd, socketIdentity);
        }else{
        	logger.log(Level.INFO, "Unexpected artifact type '"+parsedArtifactIdentity+"' in 'accept' syscall. event id '"+eventData.get("eventid")+"'");
        	return;
        }        
        
        //if reached this point then can process the accept event 
        
        ArtifactIdentity artifactIdentity = descriptors.getDescriptor(pid, fd);
        if (artifactIdentity != null) { //well shouldn't be null since all cases handled above but for future code changes
        	Process process = putProcess(eventData);
        	getArtifactProperties(artifactIdentity).markNewEpoch(eventData.get("eventid"));
            Artifact socket = putArtifact(eventData, artifactIdentity, false);
            Used used = new Used(process, socket);
            used.addAnnotation("time", time);
            used.addAnnotation("operation", getOperation(syscall));
            addEventIdAndSourceAnnotationToEdge(used, eventData.get("eventid"), DEV_AUDIT);
            putEdge(used);
        }else{
        	logger.log(Level.INFO, "No artifact found to 'accept' on. event id '"+eventData.get("eventid")+"'");
        }
    }

    private void handleSend(Map<String, String> eventData, SYSCALL syscall) {
    	// sendto()/sendmsg() receive the following message(s):
        // - SYSCALL
        // - EOE
        String pid = eventData.get("pid");
        Process process = putProcess(eventData);

        String fd = eventData.get("a0");
        String time = eventData.get("time");
        String bytesSent = eventData.get("exit");
        
        if(descriptors.getDescriptor(pid, fd) == null){
        	descriptors.addUnknownDescriptor(pid, fd); 
        	getArtifactProperties(descriptors.getDescriptor(pid, fd)).markNewEpoch(eventData.get("eventid"));
        }else{
        	if(UnixSocketIdentity.class.equals(descriptors.getDescriptor(pid, fd).getClass()) && !UNIX_SOCKETS){
        		return;
        	}
        }
       
        ArtifactIdentity artifactInfo = descriptors.getDescriptor(pid, fd);
        
        Artifact vertex = putArtifact(eventData, artifactInfo, true);
        WasGeneratedBy wgb = new WasGeneratedBy(vertex, process);
        wgb.addAnnotation("operation", getOperation(syscall));
        wgb.addAnnotation("time", time);
        wgb.addAnnotation("size", bytesSent);
        addEventIdAndSourceAnnotationToEdge(wgb, eventData.get("eventid"), DEV_AUDIT);
        putEdge(wgb);
    }
    
    private void handleRecv(Map<String, String> eventData, SYSCALL syscall) {
    	// recvfrom()/recvmsg() receive the following message(s):
        // - SYSCALL
        // - EOE
        String pid = eventData.get("pid");
        
        Process process = putProcess(eventData);

        String fd = eventData.get("a0");
        String time = eventData.get("time");
        String bytesReceived = eventData.get("exit");

        if(descriptors.getDescriptor(pid, fd) == null){
        	descriptors.addUnknownDescriptor(pid, fd); 
        	getArtifactProperties(descriptors.getDescriptor(pid, fd)).markNewEpoch(eventData.get("eventid"));
        }else{
        	if(UnixSocketIdentity.class.equals(descriptors.getDescriptor(pid, fd).getClass()) && !UNIX_SOCKETS){
        		return;
        	}
        }
        
        ArtifactIdentity artifactInfo = descriptors.getDescriptor(pid, fd);     
        
        Artifact vertex = putArtifact(eventData, artifactInfo, false);
    	Used used = new Used(process, vertex);
        used.addAnnotation("operation", getOperation(syscall));
        used.addAnnotation("time", time);
        used.addAnnotation("size", bytesReceived);
        addEventIdAndSourceAnnotationToEdge(used, eventData.get("eventid"), DEV_AUDIT);
        putEdge(used);
    }
    
    /**
     * Adding {@link #EVENT_ID EVENT_ID} and {@link #SOURCE SOURCE} annotations to the edge if not null
     * 
     * @param edge edge to add annotations to
     * @param eventId the event id in the audit log
     * @param source the source of the edge. 
     */    
    private void addEventIdAndSourceAnnotationToEdge(AbstractEdge edge, String eventId, String source){
    	if(edge != null){
	    	if(eventId != null){
	    		edge.addAnnotation(EVENT_ID, eventId);
	    	}
	    	if(source != null){
	    		edge.addAnnotation(SOURCE, source);
	    	}
    	}
    }
    
    /**
     * Creates the artifact according to the rules decided on for the current version of Audit reporter
     * and then puts the artifact in the buffer at the end if it wasn't put before.
     * 
     * @param eventData a map that contains the keys eventid, time, and pid. Used for creating the UPDATE edge. 
     * @param artifactIdentity artifact to create
     * @param updateVersion true or false to tell if the version has to be updated. Is modified based on the rules in the function
     * @return the created artifact
     */
    private Artifact putArtifact(Map<String, String> eventData, ArtifactIdentity artifactIdentity,
    								boolean updateVersion){
    	return putArtifact(eventData, artifactIdentity, updateVersion, null);
    }
    
    /**
     * Creates the artifact according to the rules decided on for the current version of Audit reporter
     * and then puts the artifact in the buffer at the end if it wasn't put before.
     * 
     * Rules:
     * 1) If unix socket identity and unix sockets disabled using {@link #UNIX_SOCKETS UNIX_SOCKETS} then null returned
     * 2) If useThisSource param is null then {@link #DEV_AUDIT DEV_AUDIT} is used
     * 3) If file identity, pipe identity or unix socket identity and path starts with /dev then set updateVersion to false
     * 4) If network socket identity versioning is false then set updateVersion to false
     * 5) If memory identity then don't put the epoch annotation
     * 6) Put vertex to buffer if not added before. We know if it is added before or not based on updateVersion and if version wasn't initialized before this call
     * 7) Draw version update edge if file identity and version has been updated
     * 
     * @param eventData a map that contains the keys eventid, time, and pid. Used for creating the UPDATE edge. 
     * @param artifactIdentity artifact to create
     * @param updateVersion true or false to tell if the version has to be updated. Is modified based on the rules in the function
     * @param useThisSource the source value to use. if null then {@link #DEV_AUDIT DEV_AUDIT} is used.
     * @return the created artifact
     */
    private Artifact putArtifact(Map<String, String> eventData, ArtifactIdentity artifactIdentity, 
    								boolean updateVersion, String useThisSource){
    	if(artifactIdentity == null || (artifactIdentity.getClass().equals(UnixSocketIdentity.class) && !UNIX_SOCKETS)){
    		return null;
    	}
    
    	ArtifactProperties artifactProperties = getArtifactProperties(artifactIdentity);

    	Artifact artifact = new Artifact();
    	artifact.addAnnotation("subtype", artifactIdentity.getSubtype().toString().toLowerCase());
    	artifact.addAnnotations(artifactIdentity.getAnnotationsMap());
    	
    	if(useThisSource != null){
    		artifact.addAnnotation(SOURCE, useThisSource);
    	}else{
    		artifact.addAnnotation(SOURCE, DEV_AUDIT);
    	}

    	Class<? extends ArtifactIdentity> artifactIdentityClass = artifactIdentity.getClass();
    	if(FileIdentity.class.equals(artifactIdentityClass)
    			|| NamedPipeIdentity.class.equals(artifactIdentityClass)
    			|| UnixSocketIdentity.class.equals(artifactIdentityClass)){
    		String path = artifact.getAnnotation("path");
    		if(path != null){
	    		if(updateVersion && path.startsWith("/dev/")){ //need this check for path based identities
	            	updateVersion = false;
	            }
    		}
    	}
    	
    	if(NetworkSocketIdentity.class.equals(artifactIdentityClass)){ //if network socket and if no version then don't update version
    		if(!NET_SOCKET_VERSIONING){
    			updateVersion = false;
    		}
    	}
    	
    	//version is always uninitialized if the epoch has been seen so using that to infer about epoch
    	boolean vertexNotSeenBefore = updateVersion || artifactProperties.isVersionUninitialized(); //do this before getVersion because it updates it based on updateVersion flag
    	
    	artifact.addAnnotation("version", String.valueOf(artifactProperties.getVersion(updateVersion)));
    	
    	if(!MemoryIdentity.class.equals(artifactIdentityClass)){ //epoch for everything except memory
    		artifact.addAnnotation("epoch", String.valueOf(artifactProperties.getEpoch()));
    	}   	
    	
    	if(vertexNotSeenBefore){//not seen because of either it has been updated or it is the first time it is seen
    		putVertex(artifact);
    	}
    	
    	//always at the end after the vertex has been added
    	if(updateVersion && FileIdentity.class.equals(artifactIdentity.getClass())){ //put the version update edge if version updated for a file
    		if(eventData != null){
    			putVersionUpdateEdge(artifact, eventData.get("time"), eventData.get("eventid"), eventData.get("pid"));
    		}else{
    			logger.log(Level.WARNING, "Failed to create version update for artifact '" +artifact + "' because time, eventid and pid missing");
    		}
    	}
    	    	
    	return artifact;
    }
    
    /**
     * Returns artifact properties for the given artifact identity. If there is no entry for the artifact identity
     * then it adds one for it and returns that. Simply observing it would modify it. Access the data structure 
     * {@link #artifactIdentityToArtifactProperties artifactIdentityToArtifactProperties} directly if need to see
     * if an entry for the given key exists.
     * 
     * @param artifactIdentity artifact identity object to get properties of
     * @return returns artifact properties in the map
     */
    private ArtifactProperties getArtifactProperties(ArtifactIdentity artifactIdentity){
    	ArtifactProperties artifactProperties = artifactIdentityToArtifactProperties.get(artifactIdentity);
    	if(artifactProperties == null){
    		artifactProperties = new ArtifactProperties();
    	}
    	artifactIdentityToArtifactProperties.put(artifactIdentity, artifactProperties);
    	return artifactProperties;
    }
    
    /**
     * Utility function to create a WasTriggeredByEdge
     * 
     * @param sourceVertex source process vertex of edge
     * @param destinationVertex destination process vertex of the edge
     * @param eventId event id as gotten in the audit log
     * @param time time as gotten in the audit log
     * @param source the source of the edge i.e. {@link #DEV_AUDIT DEV_AUDIT}, {@link #BEEP BEEP} or {@link #PROCFS PROCFS}
     * @param operation operation done in the syscall
     */
    private void putWasTriggeredByEdge(Process sourceVertex, Process destinationVertex, String eventId, String time, String source, String operation){
    	if(sourceVertex == null || destinationVertex == null){
    		logger.log(Level.WARNING, "Source {0} or destination {1} vertex null for WasTriggeredBy edge.", new Object[]{String.valueOf(sourceVertex), String.valueOf(destinationVertex)});
    		return;
    	}
    	WasTriggeredBy edge = new WasTriggeredBy(sourceVertex, destinationVertex);
		addEventIdAndSourceAnnotationToEdge(edge, eventId, DEV_AUDIT);
		if(time != null){
			edge.addAnnotation("time", time);
		}
		if(operation != null){
			edge.addAnnotation("operation", operation);
		}
		putEdge(edge);
    }
    
    /**
     * Utility function to get a new map with the existing key-values and given key-values replaced. 
     * 
     * @param keyValues existing key-values
     * @param newKeyValues key-values to update or replace
     * @return a new map with existing and updated key-values
     */    
    private Map<String, String> updateKeyValuesInMap(Map<String, String> keyValues, Map<String, String> newKeyValues){
    	Map<String, String> result = new HashMap<String, String>();
    	if(keyValues != null){
    		result.putAll(keyValues);
    	}
    	if(newKeyValues != null){
    		result.putAll(newKeyValues);
    	}
    	return result;
    }
    
    /**
     * Converts hex string as UTF-8
     * 
     * @param hexString string to parse
     * @return parsed string
     */
    private String parseHexStringToUTF8(String hexString){
    	if(hexString == null){
    		return null;
    	}
		ByteBuffer bytes = ByteBuffer.allocate(hexString.length()/2);
    	for(int a = 0; a<hexString.length()-2; a+=2){
    		bytes.put((byte)Integer.parseInt(hexString.substring(a, a+2), 16));

    	}
    	bytes.rewind();
    	Charset cs = Charset.forName("UTF-8");
    	CharBuffer cb = cs.decode(bytes);
    	return cb.toString();
	}
    
    /**
     * Returns either NetworkSocketIdentity or UnixSocketIdentity depending on the type in saddr.
     * 
     * If saddr starts with 01 then unix socket
     * If saddr starts with 02 then ipv4 network socket
     * If saddr starts with 0A then ipv6 network socket
     * If none of the above then null returned
     * 
     * Syscall parameter is using to identify (in case of network sockets) whether the address and port are 
     * source or destination ones.
     * 
     * @param saddr a hex string of format 0100... or 0200... or 0A...
     * @param syscall syscall in which this saddr was received
     * @return the appropriate subclass of ArtifactIdentity or null
     */
    private ArtifactIdentity parseSaddr(String saddr, SYSCALL syscall){
    	if(saddr != null && saddr.length() >= 2){
	    	if(saddr.charAt(1) == '1'){ //unix socket
	    		
	    		String path = "";
	        	int start = saddr.indexOf("2F"); //2F in ASCII is '/'. so starting from there since unix file paths start from there
	        	
	        	if(start != -1){ //found
	        		try{
		        		for(; start < saddr.length() - 2; start+=2){
		        			char c = (char)(Integer.parseInt(saddr.substring(start, start+2), 16));
		        			if(c == 0){ //null char
		        				break;
		        			}
		        			path += c;
		        		}
	        		}catch(Exception e){
	        			logger.log(Level.INFO, "Failed to parse saddr value '"+saddr+"'");
	        			return null;
	        		}
	        	}
	    		
	        	if(path != null && !path.isEmpty()){
	        		return new UnixSocketIdentity(path);
	        	}
	        }else{ //ip
	    	
	        	String address = null, port = null;
		    	if (saddr.charAt(1) == '2') {
		            port = Integer.toString(Integer.parseInt(saddr.substring(4, 8), 16));
		            int oct1 = Integer.parseInt(saddr.substring(8, 10), 16);
		            int oct2 = Integer.parseInt(saddr.substring(10, 12), 16);
		            int oct3 = Integer.parseInt(saddr.substring(12, 14), 16);
		            int oct4 = Integer.parseInt(saddr.substring(14, 16), 16);
		            address = String.format("%d.%d.%d.%d", oct1, oct2, oct3, oct4);
		        }else if(saddr.charAt(1) == 'A' || saddr.charAt(1) == 'a'){
		        	port = Integer.toString(Integer.parseInt(saddr.substring(4, 8), 16));
		        	int oct1 = Integer.parseInt(saddr.substring(40, 42), 16);
		        	int oct2 = Integer.parseInt(saddr.substring(42, 44), 16);
		        	int oct3 = Integer.parseInt(saddr.substring(44, 46), 16);
		        	int oct4 = Integer.parseInt(saddr.substring(46, 48), 16);
		        	address = String.format("::%s:%d.%d.%d.%d", saddr.substring(36, 40).toLowerCase(), oct1, oct2, oct3, oct4);
		        }
		    	
		    	if(address != null && port != null){
		    		if(syscall == SYSCALL.BIND){ //put address as destination
		    			return new NetworkSocketIdentity("", "", address, port, "");
		    		}else if(syscall == SYSCALL.CONNECT){ //put address as destination
		    			return new NetworkSocketIdentity("", "", address, port, "");
		    		}else if(syscall == SYSCALL.ACCEPT || syscall == SYSCALL.ACCEPT4){ //put address as source
		    			return new NetworkSocketIdentity(address, port, "", "", "");
		    		}else{
		    			logger.log(Level.INFO, "Unsupported syscall '"+syscall+"' to parse saddr '"+saddr+"'");
		    		}
		    	}
		    	
	        }
    	}
    	
    	return null;
    }
     
    /**
     * Tries to create an absolute path using the parentDirectoryPath param. If path param is absolute to begin with
     * then it returns it. If not then it concatenates if with the parent directory's path and returns that.
     *
     * @param parentDirectoryPath path of the parent directory
     * @param path path on the file system
     * @return absolute path on the file system. Null if path is null or if path is not absolute and parentDirectoryPath is null
     */
    private String constructAbsolutePathIfNotAbsolute(String parentDirectoryPath, String path){
    	if(path == null){
    		return null;
    	}
    	if(!path.startsWith(File.separator)){ //is not absolute
    		//what kind of not absolute
    		if(path.startsWith("." + File.separatorChar)){
    			path = path.substring(2);
    		}
    		//path doesn't start with '/' (for unix-based) if here
    		//do the constructing
    		if(parentDirectoryPath == null){
    			return null;
    		}
    		if (parentDirectoryPath.endsWith(File.separator)) {
                return parentDirectoryPath + path;
            } else {
                return parentDirectoryPath + File.separatorChar + path;
            }
    	}
    	return path;
    }
    
    /**
     * Creates a version update edge i.e. from the new version of an artifact to the old version of the same artifact.
     * At the moment only being done for file artifacts. See {@link #putArtifact(Map, ArtifactIdentity, boolean, String) putArtifact}.
     * 
     * If the previous version number of the artifact is less than 0 then the edge won't be drawn because that means that there was
     * no previous version. 
     * 
     * @param newArtifact artifact which has the updated version
     * @param time timestamp when this happened
     * @param eventId event id of the new version of the artifact creation
     * @param pid pid of the process which did the update
     */
    private void putVersionUpdateEdge(Artifact newArtifact, String time, String eventId, String pid){
    	if(newArtifact == null || time == null || eventId == null || pid == null){
    		logger.log(Level.WARNING, "Invalid arguments. newArtifact="+newArtifact+", time="+time+", eventId="+eventId+", pid="+pid);
    		return;
    	}
    	Artifact oldArtifact = new Artifact();
    	oldArtifact.addAnnotations(newArtifact.getAnnotations());
    	Long oldVersion = null;
    	try{
    		//this takes care of the case where not to put version update in case an epoch has happened because on epoch version is reset to 0. 
    		//so, there would be no previous version to update from.
    		oldVersion = CommonFunctions.parseLong(newArtifact.getAnnotation("version"), -1L) - 1;
    		if(oldVersion < 0){ //i.e. no previous one, it is the first artifact for the path
    			return;
    		}
    	}catch(Exception e){
    		logger.log(Level.WARNING, "Failed to create version update edge between (" + newArtifact.toString() + ") and ("+oldArtifact.toString()+")" , e);
    		return;
    	}
    	oldArtifact.addAnnotation("version", String.valueOf(oldVersion));
    	WasDerivedFrom versionUpdate = new WasDerivedFrom(newArtifact, oldArtifact);
    	versionUpdate.addAnnotation("pid", pid);
    	versionUpdate.addAnnotation("operation", getOperation(SYSCALL.UPDATE));
    	versionUpdate.addAnnotation("time", time);
    	addEventIdAndSourceAnnotationToEdge(versionUpdate, eventId, DEV_AUDIT);
    	putEdge(versionUpdate);
    }

    /**
     * Groups system call names by functionality and returns that name to simplify identification of the type of system call.
     * Grouping only done if {@link #SIMPLIFY SIMPLIFY} is true otherwise the system call name is returned simply.
     * 
     * @param syscall system call to get operation for
     * @return operation corresponding to the syscall
     */
    private String getOperation(SYSCALL syscall){
    	if(syscall == null){
    		return null;
    	}
    	SYSCALL returnSyscall = syscall;
    	if(SIMPLIFY){
    		switch (syscall) {
	    		case PIPE:
	    		case PIPE2:
	    			returnSyscall = SYSCALL.PIPE;
	    			break;
	    		case EXIT:
	    		case EXIT_GROUP:
	    			returnSyscall = SYSCALL.EXIT;
	    			break;
	    		case DUP:
	    		case DUP2:
	    		case DUP3:
	    			returnSyscall = SYSCALL.DUP;
	    			break;
	    		case BIND:
	    			returnSyscall = SYSCALL.BIND;
	    			break;
	    		case MKNOD:
	    		case MKNODAT:
	    			returnSyscall = SYSCALL.MKNOD;
	    			break;
	    		case UNKNOWN:
	    			returnSyscall = SYSCALL.UNKNOWN;
	    			break;
	    		case UPDATE:
	    			returnSyscall = SYSCALL.UPDATE;
	    			break;
	    		case EXECVE:
	    			returnSyscall = SYSCALL.EXECVE;
	    			break;
	    		case UNIT:
	    			returnSyscall = SYSCALL.UNIT;
	    			break;
    			case RENAME:
	    			returnSyscall = SYSCALL.RENAME;
	    			break;
    			case CREATE:
	    			returnSyscall = SYSCALL.CREATE;
	    			break;
    			case MMAP:
    			case MMAP2:
    				returnSyscall = SYSCALL.MMAP;
    				break;
    			case MPROTECT:
    				returnSyscall = SYSCALL.MPROTECT;
    				break;
    			case LOAD:
    				returnSyscall = SYSCALL.LOAD;
    				break;
    			case OPEN:
    			case OPENAT:
    			case CREAT:
    				returnSyscall = SYSCALL.OPEN;
    				break;
				case FORK:
				case VFORK:
					returnSyscall = SYSCALL.FORK;
					break;
				case CLONE:
					returnSyscall = SYSCALL.CLONE;
					break;
				case CHMOD:
				case FCHMOD:
					returnSyscall = SYSCALL.CHMOD;
					break;
				case SENDTO:
				case SENDMSG:
					returnSyscall = SYSCALL.SEND;
					break;
				case RECVFROM:
				case RECVMSG:
					returnSyscall = SYSCALL.RECV;
					break;
				case TRUNCATE:
				case FTRUNCATE:
					returnSyscall = SYSCALL.TRUNCATE;
					break;
				case READ:
				case READV:
				case PREAD64:
					returnSyscall = SYSCALL.READ;
					break;
				case WRITE:
				case WRITEV:
				case PWRITE64:
					returnSyscall = SYSCALL.WRITE;
					break;
				case ACCEPT:
				case ACCEPT4:
					returnSyscall = SYSCALL.ACCEPT;
					break;
				case CONNECT:
					returnSyscall = SYSCALL.CONNECT;
					break;
				case SYMLINK:
				case LINK:
					returnSyscall = SYSCALL.LINK;
					break;
				case SETUID:
				case SETREUID:
				case SETRESUID:
					returnSyscall = SYSCALL.SETUID;
					break;
				case KILL:
					returnSyscall = SYSCALL.KILL;
					break;
				default:
					break;
			}
    	}
    	return returnSyscall.toString().toLowerCase();
    }
    
    /**
     * Checks the internal data structure for artifacts to see if a path-based artifact identity with the same path exists.
     * If multiple type of artifact identities with the same path exist then it returns the one which was created last i.e. with
     * the biggest event id.
     * 
     * If unable to find an artifact identity with the given path then it returns a file artifact identity with that path. 
     * Rule: Everything is a file until proven otherwise.
     * 
     * Note: The internal data structure {@link #artifactIdentityToArtifactProperties artifactIdentityToArtifactProperties} 
     * has to be accessed directly in this function instead of using the function {@link #getArtifactProperties(ArtifactIdentity) 
     * getArtifactProperties} because that function adds and returns the properties for the artifact identity even if doesn't exist.
     * Using that function would say something existed which didn't exist before.
     * 
     * @param path path to check against
     * @return the artifact identity with the matched path or a file artifact identity if path didn't match any artifact identity
     */
    private ArtifactIdentity getValidArtifactIdentityForPath(String path){
    	FileIdentity fileIdentity = new FileIdentity(path);
    	NamedPipeIdentity namedPipeIdentity = new NamedPipeIdentity(path);
    	UnixSocketIdentity unixSocketIdentity = new UnixSocketIdentity(path);
    	//NOTE: get them directly without using the utility function. done to get null properties if not initialized yet.
    	//dont use the getArtifactProperties function here
    	ArtifactProperties fileProperties = artifactIdentityToArtifactProperties.get(fileIdentity); 
    	ArtifactProperties namedPipeProperties = artifactIdentityToArtifactProperties.get(namedPipeIdentity);
    	ArtifactProperties unixSocketProperties = artifactIdentityToArtifactProperties.get(unixSocketIdentity);
    	
    	fileProperties = fileProperties == null ? new ArtifactProperties() : fileProperties;
    	namedPipeProperties = namedPipeProperties == null ? new ArtifactProperties() : namedPipeProperties;
    	unixSocketProperties = unixSocketProperties == null ? new ArtifactProperties() : unixSocketProperties;
    	
    	long fileCreationEventId = fileProperties.getCreationEventId();
		long namedPipeCreationEventId = namedPipeProperties.getCreationEventId();
		long unixSocketCreationEventId = namedPipeProperties.getCreationEventId();

		//creation event ids won't be same unless two or more haven't been initialized yet. the uninitialized value would just be equal 
		
		if(fileCreationEventId >= namedPipeCreationEventId && fileCreationEventId >= unixSocketCreationEventId){ //always first because if all equals then we want file
			return fileIdentity;
		}else if(namedPipeCreationEventId >= fileCreationEventId && namedPipeCreationEventId >= unixSocketCreationEventId) {
			return namedPipeIdentity; 
		}else if(unixSocketCreationEventId >= fileCreationEventId && unixSocketCreationEventId >= namedPipeCreationEventId){
			return unixSocketIdentity; 
		}else{
			return fileIdentity;
		}
    }
    
    /**
     * Every path record in Audit log has a key 'nametype' which can be used to identify what the artifact
     * referenced in the path record was.
     * 
     * Possible options for nametype:
     * 1) PARENT -> parent directory of the path
     * 2) CREATE -> path was created
     * 3) NORMAL -> path was just used for read or write
     * 4) DELETE -> path was deleted
     * 5) UNKNOWN -> can't tell what was done with the path. So far seen that it only happens when a syscall fails.
     *  
     * @param eventData eventData that contains the paths information in the format: path[Index], nametype[Index]. example, path0, nametype0 
     * @param nametypeValue one of the above-mentioned values. Case sensitive compare operation on nametypeValue
     * @return returns a Map where key is the index and the value is the path name.
     */
    private Map<Integer, String> getPathsWithNametype(Map<String, String> eventData, String nametypeValue){
    	Map<Integer, String> indexToPathMap = new HashMap<Integer, String>();
    	if(eventData != null && nametypeValue != null){
    		Long items = CommonFunctions.parseLong(eventData.get("items"), 0L);
    		for(int itemcount = 0; itemcount < items; itemcount++){
    			if(nametypeValue.equals(eventData.get("nametype"+itemcount))){
    				indexToPathMap.put(itemcount, eventData.get("path"+itemcount));
    			}
    		}
    	}    	
    	return indexToPathMap;
    }
    
    //////////////////////////////////////// Process and Unit management code below
    
    /**
     * The eventData is used to get the pid and then get the process vertex if it already exists.
     * If the vertex didn't exist then it creates the new one and returns that. The new vertex is created
     * using the information in the eventData. The putVertex function is also called on the vertex if
     * it was created in this function and the new vertex is also added to the internal process map.
     * 
     * @param eventData key value pairs to use to create vertex from. This is in most cases all the key values
     * gotten in a syscall audit record. But additional information can also be added to this to be used by 
     * createProcessVertex function.
     * @return the vertex with the pid in the eventData map
     */
    private Process putProcess(Map<String, String> eventData){
    	return putProcess(eventData, false);
    }
    
    
    /**
     * See {@link #putProcess(Map) putProcess}
     * 
     * @param annotations key value pairs to use to create vertex from. This is in most cases all the key values
     * gotten in a syscall audit record. But additional information can also be added to this to be used by 
     * createProcessVertex function.
     * @param recreateAndReplace if true, it would create a new vertex using information in the eventData and 
     * replace the existing one if there is one. If false, it would recreate the vertex. 
     * @return
     */
    private Process putProcess(Map<String, String> annotations, boolean recreateAndReplace){
    	if(annotations != null){
	    	String pid = annotations.get("pid");
	    	Process process = getProcess(pid);
	        if(process == null || recreateAndReplace){
	        	process = createProcessVertex(annotations);
	        	addProcess(pid, process);
	        	putVertex(process);
	        }
	        return process;
    	}
    	return null;
    }
    
    /**
     * A convenience wrapper of 
     * {@link #createProcessVertex(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) createProcessVertex}.
     * 
     * Takes out the values for keys required by the above referenced function and calls it
     * 
     * Note: Values NOT in eventData when received from audit log -> commandline, cwd, SOURCE, start time, unit, iteration, and count.
     * So, put them manually in the map before calling this function.
     * 
     * @param annotations a key value map that usually contains the key values gotten in the audit syscall record
     * @return returns a Process instance with the passed in annotations
     */    
    private Process createProcessVertex(Map<String, String> annotations){
    	if(annotations == null){
    		return null;
    	}
    	return createProcessVertex(annotations.get("pid"), annotations.get("ppid"), 
    			//if data from audit log then 'name' is 'comm' otherwise audit annotation key is 'name'
    			annotations.get("name") == null ? annotations.get("comm") : annotations.get("name"), 
    			annotations.get("commandline"),
    			annotations.get("cwd"), annotations.get("uid"), annotations.get("euid"), annotations.get("suid"), annotations.get("fsuid"), 
    			annotations.get("gid"), annotations.get("egid"), annotations.get("sgid"), annotations.get("fsgid"), annotations.get(SOURCE), 
    			annotations.get("start time"), annotations.get("unit"), annotations.get("iteration"), annotations.get("count"));
    }
    
    /**
     * The function to be used always to create a Process vertex. Don't use 'new Process()' invocation directly under any circumstances.
     * 
     * The function applies some rules on various annotations based on global variables. Always use this function to keep things consistent.
     * 
     * @param pid pid of the process
     * @param ppid parent's pid of the process
     * @param name name or 'comm' of the process as in Audit log syscall record
     * @param commandline gotten through execve syscall event only or when copied from an existing one [Optional]
     * @param cwd current working directory of the process [Optional]
     * @param uid user id of the process
     * @param euid effective user id of the process
     * @param suid saved user id of the process
     * @param fsuid file system user id of the process
     * @param gid group id of the process
     * @param egid effective group id of the process
     * @param sgid saved group id of the process
     * @param fsgid file system group id of the process
     * @param source source of the process information: /dev/audit, beep or /proc only. Defaults to /dev/audit if none given.
     * @param startTime time at which the process was created [Optional]
     * @param unit id of the unit loop. [Optional]
     * @param iteration iteration of the unit loop. [Optional]
     * @param count count of a unit with all other annotation values same. [Optional]
     * @return returns a Process instance with the passed in annotations.
     */
    private Process createProcessVertex(String pid, String ppid, String name, String commandline, String cwd, 
    		String uid, String euid, String suid, String fsuid, 
    		String gid, String egid, String sgid, String fsgid,
    		String source, String startTime, String unit, String iteration, String count){
    	
    	Process process = new Process();
    	process.addAnnotation("pid", pid);
    	process.addAnnotation("ppid", ppid);
    	process.addAnnotation("name", name);
    	process.addAnnotation("uid", uid);
    	process.addAnnotation("euid", euid);
    	process.addAnnotation("gid", gid);
    	process.addAnnotation("egid", egid);
    	
    	if(source == null){
    		process.addAnnotation(SOURCE, DEV_AUDIT);
    	}else{
    		process.addAnnotation(SOURCE, source);
    	}
    	
    	//optional annotations below:
    	
    	if(commandline != null){
    		process.addAnnotation("commandline", commandline);
    	}
    	if(cwd != null){
    		process.addAnnotation("cwd", cwd);
    	}
    	
    	if(!SIMPLIFY){
        	process.addAnnotation("suid", suid);
        	process.addAnnotation("fsuid", fsuid);
        	
        	process.addAnnotation("sgid", sgid);
        	process.addAnnotation("fsgid", fsgid);
    	}
    	
    	if(startTime != null){
    		process.addAnnotation("start time", startTime);
    	}
    	
    	if(CREATE_BEEP_UNITS){
    		if(unit == null){
    			process.addAnnotation("unit", "0"); // 0 indicates containing process
    		}else{
    			process.addAnnotation("unit", unit);
    			// The iteration and count annotations are only for units and not the containing process
    			if(iteration != null){
        			process.addAnnotation("iteration", iteration);
        		}
        		if(count != null){
        			process.addAnnotation("count", count);
        		}
    		}
    	}
    	
    	return process;
    }

    /**
     * Creates a process object by reading from the /proc FS. Calls the
 	 * {@link #createProcessVertex(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)}
 	 * internally to create the process vertex.
 	 * 
     * @param pid pid of the process to look for in the /proc FS
     * @return a Process object populated using /proc FS
     */
    private Process createProcessFromProcFS(String pid) {
        // Check if this pid exists in the /proc/ filesystem
        if ((new java.io.File("/proc/" + pid).exists())) {
            // The process vertex is created using the proc filesystem.
            try {
                // order of keys in the status file changed. So, now looping through the file to get the necessary ones
                int keysGottenCount = 0; //used to stop reading the file once all the required keys have been gotten
                String line = null, nameline = null, ppidline = null, uidline = null, gidline = null;
                BufferedReader procReader = new BufferedReader(new FileReader("/proc/" + pid + "/status"));
                while ((line = procReader.readLine()) != null && keysGottenCount < 4) {
                    String tokens[] = line.split(":");
                    String key = tokens[0].trim().toLowerCase();
                    switch (key) {
                        case "name":
                            nameline = line;
                            keysGottenCount++;
                            break;
                        case "ppid":
                            ppidline = line;
                            keysGottenCount++;
                            break;
                        case "uid":
                            uidline = line;
                            keysGottenCount++;
                            break;
                        case "gid":
                            gidline = line;
                            keysGottenCount++;
                            break;
                        default:
                            break;
                    }
                }
                procReader.close();

                BufferedReader statReader = new BufferedReader(new FileReader("/proc/" + pid + "/stat"));
                String statline = statReader.readLine();
                statReader.close();

                BufferedReader cmdlineReader = new BufferedReader(new FileReader("/proc/" + pid + "/cmdline"));
                String cmdline = cmdlineReader.readLine();
                cmdlineReader.close();

                String stats[] = statline.split("\\s+");
                long elapsedtime = Long.parseLong(stats[21]) * 10;
                long startTime = boottime + elapsedtime;
//                String stime_readable = new java.text.SimpleDateFormat(simpleDatePattern).format(new java.util.Date(startTime));
//                String stime = Long.toString(startTime);
//
                String name = nameline.split("name:")[1].trim();
                String ppid = ppidline.split("\\s+")[1];
                cmdline = (cmdline == null) ? "" : cmdline.replace("\0", " ").replace("\"", "'").trim();

                // see for order of uid, euid, suid, fsiud: http://man7.org/linux/man-pages/man5/proc.5.html
                String gidTokens[] = gidline.split("\\s+");
                String uidTokens[] = uidline.split("\\s+");
                
                Process newProcess = createProcessVertex(pid, ppid, name, null, null, 
                		uidTokens[1], uidTokens[2], uidTokens[3], uidTokens[4], 
                		gidTokens[1], gidTokens[2], gidTokens[3], gidTokens[4], 
                		PROC_FS, Long.toString(startTime), CREATE_BEEP_UNITS ? "0" : null, null, null);
                
                // newProcess.addAnnotation("starttime_unix", stime);
                // newProcess.addAnnotation("starttime_simple", stime_readable);
                // newProcess.addAnnotation("commandline", cmdline);
                return newProcess;
            } catch (IOException | NumberFormatException e) {
                logger.log(Level.WARNING, "Unable to create process vertex for pid " + pid + " from /proc/", e);
                return null;
            }
        } else {
            return null;
        }
    }
    
    /**
     * Returns the next iteration number for the pid and unitId combination.
     * 
     * @param pid pid of the process to which the unit would belong
     * @param unitId id of the loop of which the iteration is
     * @return the iteration number. Always greater than or equal to 0 and starts from 0
     */        
    private Long getNextIterationNumber(String pid, String unitId){
    	if(iterationNumber.get(pid) == null){
    		iterationNumber.put(pid, new HashMap<String, Long>());
    	}
    	if(iterationNumber.get(pid).get(unitId) == null){
    		iterationNumber.get(pid).put(unitId, -1L);
    	}
    	iterationNumber.get(pid).put(unitId, iterationNumber.get(pid).get(unitId) + 1);
    	return iterationNumber.get(pid).get(unitId);
    }
    
    /**
     * Returns the main process i.e. at the zeroth index in the process stack for the pid, if any.
     * 
     * @param pid pid of the process
     * @return the main process with unit id 0. null if none found.
     */
    private Process getContainingProcessVertex(String pid){
    	if(processUnitStack.get(pid) != null && processUnitStack.get(pid).size() > 0){
    		return processUnitStack.get(pid).getFirst();
    	}
    	return null;
    }
    
    /**
     * This function resets the stack for a pid used to keep the main containing process for a pid
     * and all the units for that pid. It clears the stack. Only call this when it is intended to clear 
     * all the state related to a process, state being the units and the existing process. Example, execve.
     *
     * @param pid pid of the process to return
     * @param process the process to add against the pid
     */
    private void addProcess(String pid, Process process){ 
    	if(pid == null || process == null){
    		logger.log(Level.WARNING, "Failed to add process vertex in addProcess function because pid or process passed is null. Pid {0}, Process {1}", new Object[]{pid, String.valueOf(process)});
    	}else{
	    	iterationNumber.remove(pid); //start iteration count from start
	    	processUnitStack.put(pid, new LinkedList<Process>()); //always reset the stack whenever the main process is being added
	    	processUnitStack.get(pid).addFirst(process);
    	}
    }
    
    /**
     * Checks the internal process hashmap and returns the process vertex. 
     * The process hashmap must have at least one vertex for that pid to return with not null.
     * The function returns the process vertex whichever is on top of the stack using a peek i.e.
     * can be a unit iteration
     * 
     * @param pid pid of the process to return
     * @return process process with the given pid or null (if none found)
     */
    private Process getProcess(String pid){
    	if(processUnitStack.get(pid) != null && !processUnitStack.get(pid).isEmpty()){
    		Process process = processUnitStack.get(pid).peekLast();
    		return process;
    	}
    	return null;
    }
    
    /**
     * Creates a unit iteration vertex, pushes it onto the vertex stack and calls putVertex on the created vertex.
     * Before adding this new unit iteration the previous one is removed from the stack.
     * The new unit iteration vertex is a copy of the annotation process with a new iteration number.
     * Furthermore, a count annotation is used to distinguish between the ith iteration of unitId x and the jth iteration 
     * of unitId x where i=j and a unit end was seen between the ith and the jth iteration, all in one timestamp of audit 
     * log.
     * 
     * Finally, the created unit iteration is added to the memory stack and putVertex is called on it.
     * 
     * @param pid the pid of the process which created this unit iteration
     * @param unitId the id of the unit as gotten from the audit log
     * @param startTime the time at which this unit was created
     * @return the created unit iteration vertex. null if there was no containing process for the pid
     */
    private Process pushUnitIterationOnStack(String pid, String unitId, String startTime){
    	if("0".equals(unitId)){
    		return null; //unit 0 is containing process and cannot have iterations
    	}
    	Process containingProcess = getContainingProcessVertex(pid);
    	if(containingProcess == null){
    		return null;
    	}
    	//get next iteration number and remove the previous iteration (if any) for the pid and unitId combination
    	Long nextIterationNumber = getNextIterationNumber(pid, unitId);
    	if(nextIterationNumber > 0){ //if greater than zero then remove otherwise this is the first one
    		removePreviousIteration(pid, unitId, nextIterationNumber);
    	}
    	
    	if(lastTimestamp == NOT_SET || !startTime.equals(lastTimestamp)){
			lastTimestamp = startTime;
			unitIterationRepetitionCounts.clear();
		}
    	
    	UnitVertexIdentifier unitVertexIdentifier = new UnitVertexIdentifier(pid, unitId, String.valueOf(nextIterationNumber));
		if(unitIterationRepetitionCounts.get(unitVertexIdentifier) == null){
			unitIterationRepetitionCounts.put(unitVertexIdentifier, -1);
		}
		unitIterationRepetitionCounts.put(unitVertexIdentifier, unitIterationRepetitionCounts.get(unitVertexIdentifier)+1);
		
		String count = String.valueOf(unitIterationRepetitionCounts.get(unitVertexIdentifier));
		
		Process newUnit = createBEEPCopyOfProcess(containingProcess, startTime, unitId, String.valueOf(nextIterationNumber), count); //first element is always the main process vertex
    	
		processUnitStack.get(pid).addLast(newUnit);
    	
    	putVertex(newUnit); //add to internal buffer. not calling putProcess here beacuse that would reset the stack
    	return newUnit;
    }
    
    /**
     * This function removes all the units of the given unitId and their iteration units from the process stack.
     * Also resets the iteration count for the given unitId
     * 
     *   @param pid pid of the process
     *   @param unitId id of the unit for which the iterations have to be removed
     */
    private void popUnitIterationsFromStack(String pid, String unitId){
    	if("0".equals(unitId) || unitId == null){
    		return; //unit 0 is containing process and should be removed using the addProcess method
    	}
    	if(processUnitStack.get(pid) != null){
    		int size = processUnitStack.get(pid).size() - 1;
    		for(int a = size; a>=0; a--){ //going in reverse because we would be removing elements
    			Process unit = processUnitStack.get(pid).get(a);
    			if(unitId.equals(unit.getAnnotation("unit"))){
    				processUnitStack.get(pid).remove(a);
    			}
    		}
    		if(iterationNumber.get(pid) != null){
    			iterationNumber.get(pid).remove(unitId);//start iteration count from start
    		}
    	}
    }
    
    /**
     * Only removes the previous unit iteration i.e. the currentIteration-1 iteration.
     * Used to remove the last iteration when a new iteration occurs for the pid and unitId
     * combination because each unit iteration's lifetime is only till the new one
     * 
     * @param pid pid of the process to which the unit belongs
     * @param unitId unitId of the unit whose iteration is going to be removed
     * @param currentIteration number of the iteration whose previous one needs to be removed
     */
    private void removePreviousIteration(String pid, String unitId, Long currentIteration){
    	if("0".equals(unitId) || unitId == null || currentIteration == null){
    		return; //unit 0 is containing process and should be removed using the addProcess method
    	}
    	if(processUnitStack.get(pid) != null){
    		int size = processUnitStack.get(pid).size() - 1;
    		for(int a = size; a>=0; a--){ //going in reverse because we would be removing elements
    			Process unit = processUnitStack.get(pid).get(a);
    			if(unitId.equals(unit.getAnnotation("unit"))){
    				Long iteration = CommonFunctions.parseLong(unit.getAnnotation("iteration"), currentIteration+1); 
    				//default value is currentIteration+1, so that we by mistake don't remove an iteration which didn't have a proper iteration value
    				if(iteration < currentIteration){
    					processUnitStack.get(pid).remove(a);
    					//just removing one
    					break;
    				}
    			}
    		}
    	}
    }
    
    /**
     * Creates a copy of the given process with the source annotation being 'beep'  
     * 
     * @param process process to create a copy of
     * @param startTime start time of the unit
     * @param unitId id of the unit
     * @return a copy with the copied and the updated annotations as in the process argument
     */
    private Process createBEEPCopyOfProcess(Process process, String startTime, String unitId, String iteration, String count){
    	if(process == null){
    		return null;
    	}
    	return createProcessVertex(process.getAnnotation("pid"), process.getAnnotation("ppid"), process.getAnnotation("name"), 
    			process.getAnnotation("commandline"), process.getAnnotation("cwd"), 
    			process.getAnnotation("uid"), process.getAnnotation("euid"), process.getAnnotation("suid"), process.getAnnotation("fsuid"), 
    			process.getAnnotation("gid"), process.getAnnotation("egid"), process.getAnnotation("sgid"), process.getAnnotation("fsgid"), 
    			BEEP, startTime, unitId, iteration, count);
    }
    
}

/**
 * Used to uniquely identity a unit iteration for a single timestamp. See {@link #pushUnitIterationOnStack(String, String, String) pushUnitIterationOnStack}.
 */
class UnitVertexIdentifier implements Serializable{
	
	private static final long serialVersionUID = -1582525150906258735L;
	
	private String pid, unitId, iteration;
	public UnitVertexIdentifier(String pid, String unitId, String iteration){
		this.pid = pid;
		this.unitId = unitId;
		this.iteration = iteration;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((iteration == null) ? 0 : iteration.hashCode());
		result = prime * result + ((pid == null) ? 0 : pid.hashCode());
		result = prime * result + ((unitId == null) ? 0 : unitId.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UnitVertexIdentifier other = (UnitVertexIdentifier) obj;
		if (iteration == null) {
			if (other.iteration != null)
				return false;
		} else if (!iteration.equals(other.iteration))
			return false;
		if (pid == null) {
			if (other.pid != null)
				return false;
		} else if (!pid.equals(other.pid))
			return false;
		if (unitId == null) {
			if (other.unitId != null)
				return false;
		} else if (!unitId.equals(other.unitId))
			return false;
		return true;
	}
}
