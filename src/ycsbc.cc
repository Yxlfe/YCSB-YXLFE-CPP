//
//  ycsbc.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <ctime>

#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <iomanip>  
#include <atomic>

#include "core/client.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/measurements.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/timer.h"
#include "utils/utils.h"

using namespace std;
////statistics
atomic<uint64_t> ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1];    //操作个数
atomic<uint64_t> ops_time[ycsbc::Operation::READMODIFYWRITE + 1];   //微秒
////

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props);
void PrintInfo(ycsbc::utils::Properties &props);
void Init(ycsbc::utils::Properties &props);

void StatusThread(ycsbc::Measurements *measurements, ycsbc::utils::CountDownLatch *latch, int interval) {
  using namespace std::chrono;
  time_point<system_clock> start = system_clock::now();
  bool done = false;
  while (1) {
    time_point<system_clock> now = system_clock::now();
    std::time_t now_c = system_clock::to_time_t(now);
    duration<double> elapsed_time = now - start;

    std::cout << std::put_time(std::localtime(&now_c), "%F %T") << ' '
              << static_cast<long long>(elapsed_time.count()) << " sec: ";

    std::cout << measurements->GetStatusMsg() << std::endl;

    if (done) {
      break;
    }
    done = latch->AwaitFor(interval);
  };
}

void RateLimitThread(std::string rate_file, std::vector<ycsbc::utils::RateLimiter *> rate_limiters,
                     ycsbc::utils::CountDownLatch *latch) {
  std::ifstream ifs;
  ifs.open(rate_file);

  if (!ifs.is_open()) {
    ycsbc::utils::Exception("failed to open: " + rate_file);
  }

  int64_t num_threads = rate_limiters.size();

  int64_t last_time = 0;
  while (!ifs.eof()) {
    int64_t next_time;
    int64_t next_rate;
    ifs >> next_time >> next_rate;

    if (next_time <= last_time) {
      ycsbc::utils::Exception("invalid rate file");
    }

    bool done = latch->AwaitFor(next_time - last_time);
    if (done) {
      break;
    }
    last_time = next_time;

    for (auto x : rate_limiters) {
      x->SetRate(next_rate / num_threads);
    }
  }
}

int main(const int argc, const char *argv[]) {

  ycsbc::utils::Properties props;
  Init(props);
  ParseCommandLine(argc, argv, props);

  const bool do_load = (props.GetProperty("doload", "false") == "true");
  const bool do_transaction = (props.GetProperty("dotransaction", "false") == "true");
  // const bool wait_for_balance = ycsbc::utils::StrToBool(props["dbwaitforbalance"]);

  if (!do_load && !do_transaction) {
    std::cerr << "No operation to do" << std::endl;
    exit(1);
  }

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  //测试延迟相关
  ycsbc::Measurements *measurements = ycsbc::CreateMeasurements(&props);
  if (measurements == nullptr) {
    std::cerr << "Unknown measurements name" << std::endl;
    exit(1);
  }

  //创建数据库
  std::vector<ycsbc::DB *> dbs;
  for (int i = 0; i < num_threads; i++) {
    ycsbc::DB *db = ycsbc::DBFactory::CreateDB(&props, measurements);
    if (db == nullptr) {
      std::cerr << "Unknown database name " << props["dbname"] << std::endl;
      exit(1);
    }
    dbs.push_back(db);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  // print status periodically
  const bool show_status = (props.GetProperty("status", "false") == "true");
  const int status_interval = std::stoi(props.GetProperty("status.interval", "1800"));
  const bool print_stats = (props.GetProperty("dbstatistics","false") == "true");

  // load phase
  if (do_load) {
    const int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<uint64_t, std::micro> timer;

    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread,
                                 measurements, &latch, status_interval);
    }

    std::vector<std::future<int>> client_threads;
    timer.Start();
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }

      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             thread_ops, true, true, !do_transaction, &latch, nullptr));
    }
    assert((int)client_threads.size() == num_threads);

    int sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      sum += n.get();
    }
    // uint64_t runtime_timer = timer.End();
    uint64_t runtime = timer.End();
    if (show_status) {
      status_future.wait();
    }

    // uint64_t temp_cnt = ops_cnt[ycsbc::INSERT].load(std::memory_order_relaxed);
    // uint64_t temp_time = ops_time[ycsbc::INSERT].load(std::memory_order_relaxed);
    // uint64_t runtime = temp_time;
    // std::cout << "********** temp_cnt = "<< temp_cnt << "**********" << std::endl;
    // std::cout << "test_runtime_timer = " << runtime_timer << std::endl;
    // std::cout << "test_runtime = " << runtime << std::endl;

    std::cout << "********** load result **********" << std::endl;
    std::cout << "loading records: " << sum << std::endl
              << "use time: " << std::fixed << std::setprecision(3) << 1.0 * runtime * 1e-6 << " s" 
              << std::endl
              << "IOPS: " << std::fixed << std::setprecision(2) << (1.0 * sum * 1e6 / runtime) 
              << "iops: " << std::fixed << std::setprecision(2) << (1.0 * runtime / sum) << " (us/op)"
              <<std::endl;
    std::cout << "*********************************" << std::endl;

    // printf("********** load result **********\n");
    //     printf("loading records:%d  use time:%.3f s  IOPS:%.2f iops (%.2f us/op)\n", sum, 1.0 * use_time*1e-6, 1.0 * sum * 1e6 / use_time, 1.0 * use_time / sum);
    // printf("*********************************\n");
    // std::cout << "Load runtime(sec): " << runtime << std::endl;
    // std::cout << "Load operations(ops): " << sum << std::endl;
    // std::cout << "Load throughput(ops/sec): " << sum / runtime << std::endl;
    
    try
    {
      if (print_stats) {
        printf("-------------- db statistics after load--------------\n");
        ycsbc::DB* print_db = dbs[0];
        print_db->PrintStats();
        for (int i = 1; i < num_threads; i++) {
          if (*dbs[i] != *print_db)
          {
            print_db = dbs[i];
            print_db->PrintStats();
          }        
        }
        
        printf("----------------------------------------------------\n");
      }
    }
    catch(const ycsbc::utils::Exception &e)
    {
      std::cerr << "Caught exception:" << e.what() << std::endl;
    }
    

  }

  measurements->Reset();
  std::this_thread::sleep_for(std::chrono::seconds(stoi(props.GetProperty("sleepafterload", "0"))));


  // transaction phase
  if (do_transaction) {
    // initial ops per second, unlimited if <= 0
    const int64_t ops_limit = std::stoi(props.GetProperty("limit.ops", "0"));
    // rate file path for dynamic rate limiting, format "time_stamp_sec new_ops_per_second" per line
    std::string rate_file = props.GetProperty("limit.file", "");

    const int total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<uint64_t, std::micro> timer;

    for(int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++){
      ops_cnt[j].store(0);
      ops_time[j].store(0);
    }

    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread,
                                 measurements, &latch, status_interval);
    }
    std::vector<std::future<int>> client_threads;
    std::vector<ycsbc::utils::RateLimiter *> rate_limiters;

    timer.Start();
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
      ycsbc::utils::RateLimiter *rlim = nullptr;
      if (ops_limit > 0 || rate_file != "") {
        int64_t per_thread_ops = ops_limit / num_threads;
        rlim = new ycsbc::utils::RateLimiter(per_thread_ops, per_thread_ops);
      }
      rate_limiters.push_back(rlim);
      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             thread_ops, false, !do_load, true, &latch, rlim));
    }

    std::future<void> rlim_future;
    if (rate_file != "") {
      rlim_future = std::async(std::launch::async, RateLimitThread, rate_file, rate_limiters, &latch);
    }

    assert((int)client_threads.size() == num_threads);

    int sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      sum += n.get();
    }
    uint64_t runtime = timer.End();

    if (show_status) {
      status_future.wait();
    }

    uint64_t temp_cnt[ycsbc::Operation::READMODIFYWRITE + 1];
    uint64_t temp_time[ycsbc::Operation::READMODIFYWRITE + 1];

    for(int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++){
      temp_cnt[j] = ops_cnt[j].load(std::memory_order_relaxed);
      temp_time[j] = ops_time[j].load(std::memory_order_relaxed);
    }
    // uint64_t runtime = 0;

    // for(int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++){
    //   temp_cnt[j] = ops_cnt[j].load(std::memory_order_relaxed);
    //   temp_time[j] = ops_time[j].load(std::memory_order_relaxed);
    //   runtime += temp_time[j];
    // }

    std::cout << "********** run result **********" << std::endl;
    std::cout << "all operation records: " << sum << std::endl
              << "use time: " << std::fixed << std::setprecision(3) << 1.0 * runtime * 1e-6 << " s" 
              << std::endl
              << "IOPS: " << std::fixed << std::setprecision(2) << 1.0 * sum * 1e6 / runtime 
              << std::endl
              << "iops: " << std::fixed << std::setprecision(2) << 1.0 * runtime / sum << " (us/op)" 
              << std::endl;

    if (temp_cnt[ycsbc::INSERT])
        std::cout << "insert ops: " << temp_cnt[ycsbc::INSERT] 
                  << std::endl
                  << "use time: " << std::fixed << std::setprecision(3) << 1.0 * temp_time[ycsbc::INSERT] * 1e-6 << " s"
                  << std::endl
                  << "IOPS: " << std::fixed << std::setprecision(2) << 1.0 * temp_cnt[ycsbc::INSERT] * 1e6 / temp_time[ycsbc::INSERT]
                  << std::endl
                  << "iops " << std::fixed << std::setprecision(2) << 1.0 * temp_time[ycsbc::INSERT] / temp_cnt[ycsbc::INSERT] << " (us/op)" 
                  << std::endl;

    if (temp_cnt[ycsbc::READ])
        std::cout << "read ops: " << temp_cnt[ycsbc::READ] 
                  << std::endl
                  << "use time: " << std::fixed << std::setprecision(3) << 1.0 * temp_time[ycsbc::READ] * 1e-6 << " s"
                  << std::endl
                  << "IOPS: " << std::fixed << std::setprecision(2) << 1.0 * temp_cnt[ycsbc::READ] * 1e6 / temp_time[ycsbc::READ]
                  << std::endl
                  << "iops " << std::fixed << std::setprecision(2) << 1.0 * temp_time[ycsbc::READ] / temp_cnt[ycsbc::READ] << " (us/op)" 
                  << std::endl;

    if (temp_cnt[ycsbc::UPDATE])
        std::cout << "update ops: " << temp_cnt[ycsbc::UPDATE] 
                  << std::endl
                  << "use time: " << std::fixed << std::setprecision(3) << 1.0 * temp_time[ycsbc::UPDATE] * 1e-6 << " s"
                  << std::endl
                  << "IOPS: " << std::fixed << std::setprecision(2) << 1.0 * temp_cnt[ycsbc::UPDATE] * 1e6 / temp_time[ycsbc::UPDATE]
                  << std::endl
                  << "iops " << std::fixed << std::setprecision(2) << 1.0 * temp_time[ycsbc::UPDATE] / temp_cnt[ycsbc::UPDATE] << " (us/op)" 
                  << std::endl;

    if (temp_cnt[ycsbc::SCAN])
        std::cout << "scan ops: " << temp_cnt[ycsbc::SCAN] 
                  << std::endl
                  << "use time: " << std::fixed << std::setprecision(3) << 1.0 * temp_time[ycsbc::SCAN] * 1e-6 << " s"
                  << std::endl
                  << "IOPS: " << std::fixed << std::setprecision(2) << 1.0 * temp_cnt[ycsbc::SCAN] * 1e6 / temp_time[ycsbc::SCAN]
                  << std::endl
                  << "iops " << std::fixed << std::setprecision(2) << 1.0 * temp_time[ycsbc::SCAN] / temp_cnt[ycsbc::SCAN] << " (us/op)" 
                  << std::endl;

    if (temp_cnt[ycsbc::READMODIFYWRITE])
        std::cout << "rmw ops: " << temp_cnt[ycsbc::READMODIFYWRITE] 
                  << std::endl
                  << "use time: " << std::fixed << std::setprecision(3) << 1.0 * temp_time[ycsbc::READMODIFYWRITE] * 1e-6 << " s"
                  << std::endl
                  << "IOPS: " << std::fixed << std::setprecision(2) << 1.0 * temp_cnt[ycsbc::READMODIFYWRITE] * 1e6 / temp_time[ycsbc::READMODIFYWRITE]
                  << std::endl
                  << "iops " << std::fixed << std::setprecision(2) << 1.0 * temp_time[ycsbc::READMODIFYWRITE] / temp_cnt[ycsbc::READMODIFYWRITE] << " (us/op)" 
                  << std::endl;

    std::cout << "********************************" << std::endl;

    
    // printf("********** run result **********\n");
    // printf("all opeartion records:%d  use time:%.3f s  IOPS:%.2f iops (%.2f us/op)\n\n", sum, 1.0 * use_time*1e-6, 1.0 * sum * 1e6 / use_time, 1.0 * use_time / sum);
    // if ( temp_cnt[ycsbc::INSERT] )          printf("insert ops:%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::INSERT], 1.0 * temp_time[ycsbc::INSERT]*1e-6, 1.0 * temp_cnt[ycsbc::INSERT] * 1e6 / temp_time[ycsbc::INSERT], 1.0 * temp_time[ycsbc::INSERT] / temp_cnt[ycsbc::INSERT]);
    // if ( temp_cnt[ycsbc::READ] )            printf("read ops  :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::READ], 1.0 * temp_time[ycsbc::READ]*1e-6, 1.0 * temp_cnt[ycsbc::READ] * 1e6 / temp_time[ycsbc::READ], 1.0 * temp_time[ycsbc::READ] / temp_cnt[ycsbc::READ]);
    // if ( temp_cnt[ycsbc::UPDATE] )          printf("update ops:%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::UPDATE], 1.0 * temp_time[ycsbc::UPDATE]*1e-6, 1.0 * temp_cnt[ycsbc::UPDATE] * 1e6 / temp_time[ycsbc::UPDATE], 1.0 * temp_time[ycsbc::UPDATE] / temp_cnt[ycsbc::UPDATE]);
    // if ( temp_cnt[ycsbc::SCAN] )            printf("scan ops  :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::SCAN], 1.0 * temp_time[ycsbc::SCAN]*1e-6, 1.0 * temp_cnt[ycsbc::SCAN] * 1e6 / temp_time[ycsbc::SCAN], 1.0 * temp_time[ycsbc::SCAN] / temp_cnt[ycsbc::SCAN]);
    // if ( temp_cnt[ycsbc::READMODIFYWRITE] ) printf("rmw ops   :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::READMODIFYWRITE], 1.0 * temp_time[ycsbc::READMODIFYWRITE]*1e-6, 1.0 * temp_cnt[ycsbc::READMODIFYWRITE] * 1e6 / temp_time[ycsbc::READMODIFYWRITE], 1.0 * temp_time[ycsbc::READMODIFYWRITE] / temp_cnt[ycsbc::READMODIFYWRITE]);
    // printf("********************************\n");


    // std::cout << "Run runtime(sec): " << runtime << std::endl;
    // std::cout << "Run operations(ops): " << sum << std::endl;
    // std::cout << "Run throughput(ops/sec): " << sum / runtime << std::endl;

    try
    {
      if (print_stats) {
        printf("-------------- db statistics after run--------------\n");
        ycsbc::DB* print_db = dbs[0];
        print_db->PrintStats();
        for (int i = 1; i < num_threads; i++) {
          if (*dbs[i] != *print_db)
          {
            print_db = dbs[i];
            print_db->PrintStats();
          }        
        }
        
        printf("----------------------------------------------------\n");
      }
    }
    catch(const ycsbc::utils::Exception &e)
    {
      std::cerr << "Caught exception:" << e.what() << std::endl;
    }

  }

  // if (wait_for_balance) {
  //   uint64_t sleep_time = 0;
  //   while(!db->HaveBalancedDistribution()){
  //     sleep(10);
  //     sleep_time += 10;
  //   }
  //   printf("Wait balance:%lu s\n",sleep_time);

  //   printf("-------------- db statistics --------------\n");
  //   db->PrintStats();
  //   printf("-------------------------------------------\n");
  // }

  for (int i = 0; i < num_threads; i++) {
    delete dbs[i];
  }
}

void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props) {
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-load") == 0) {
      props.SetProperty("doload", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-run") == 0 || strcmp(argv[argindex], "-t") == 0) {
      props.SetProperty("dotransaction", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -threads" << std::endl;
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -db" << std::endl;
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-dbpath") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -dbpath" << std::endl;
        exit(0);
      }
      props.SetProperty("dbpath", argv[argindex]);
      argindex++;
    }else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -P" << std::endl;
        exit(0);
      }
      // std::string filename(argv[argindex]);
      std::ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const std::string &message) {
        std::cerr << message << std::endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-p") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -p" << std::endl;
        exit(0);
      }
      std::string prop(argv[argindex]);
      size_t eq = prop.find('=');
      if (eq == std::string::npos) {
        std::cerr << "Argument '-p' expected to be in key=value format "
                     "(e.g., -p operationcount=99999)" << std::endl;
        exit(0);
      }
      props.SetProperty(ycsbc::utils::Trim(prop.substr(0, eq)),
                        ycsbc::utils::Trim(prop.substr(eq + 1)));
      argindex++;
    } else if (strcmp(argv[argindex], "-s") == 0) {
      props.SetProperty("status", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-dbstatistics") == 0) {
      props.SetProperty("dbstatistics", "true");
      argindex++;
    }else {
      UsageMessage(argv[0]);
      std::cerr << "Unknown option '" << argv[argindex] << "'" << std::endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command) {
  std::cout <<
      "Usage: " << command << " [options]\n"
      "Options:\n"
      "  -load: run the loading phase of the workload\n"
      "  -t: run the transactions phase of the workload\n"
      "  -run: same as -t\n"
      "  -threads n: execute using n threads (default: 1)\n"
      "  -db dbname: specify the name of the DB to use (default: basic)\n"
      "  -P propertyfile: load properties from the given file. Multiple files can\n"
      "                   be specified, and will be processed in the order specified\n"
      "  -p name=value: specify a property to be passed to the DB and workloads\n"
      "                 multiple properties can be specified, and override any\n"
      "                 values in the propertyfile\n"
      "  -s: print status every 10 seconds (use status.interval prop to override)"
      << std::endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void PrintInfo(ycsbc::utils::Properties &props) {
  printf("---- dbname:%s  dbpath:%s ----\n", props["dbname"].c_str(), props["dbpath"].c_str());
  printf("%s", props.DebugString().c_str());
  printf("----------------------------------------\n");
  fflush(stdout);
}

void Init(ycsbc::utils::Properties &props){
  props.SetProperty("dbname","basic");
  props.SetProperty("dbpath","");
  props.SetProperty("load","false");
  props.SetProperty("run","false");
  props.SetProperty("threadcount","1");
  props.SetProperty("dbstatistics","false");
  // props.SetProperty("dbwaitforbalance","false");
}