// Logging: daily file retention swept at startup, colourised level names in
// the file, and level filtering.

#include "test_support.h"

#include <filesystem>
#include <fstream>

#include "axon/market_data/logging.h"

namespace mds_test {
namespace {

namespace fs = std::filesystem;

class LoggingTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() / ("mds_logs_" + std::to_string(::getpid()) + "_" +
                                            ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fs::remove_all(dir_);
        fs::create_directories(dir_);
    }
    void TearDown() override {
        spdlog::drop("mds");
        fs::remove_all(dir_);
    }

    void touch(const std::string& name) { std::ofstream(dir_ / name) << "old\n"; }

    std::vector<std::string> files() const {
        std::vector<std::string> out;
        for (auto& e : fs::directory_iterator(dir_)) out.push_back(e.path().filename().string());
        std::sort(out.begin(), out.end());
        return out;
    }

    std::string today_file() const {
        auto now = std::time(nullptr);
        char buf[32];
        std::strftime(buf, sizeof buf, "mds_%Y-%m-%d.log", std::localtime(&now));
        return buf;
    }

    std::string read(const std::string& name) const {
        std::ifstream in(dir_ / name);
        return {std::istreambuf_iterator<char>(in), {}};
    }

    fs::path dir_;
};

// spdlog only prunes when it rotates, never touches a backlog that exists at
// startup, and stops at the first missing day. The startup sweep has to clean
// up after downtime with gaps in it.
TEST_F(LoggingTest, StartupSweepKeepsOnlyTheNewestFilesAcrossGaps) {
    touch("mds_2020-01-01.log");
    touch("mds_2020-01-05.log");  // a gap of days in between
    touch("mds_2021-06-30.log");
    touch("mds_2022-12-31.log");
    touch("unrelated.txt");
    touch("mds_notadate.log");

    auto logger = make_logger({(dir_ / "mds.log").string(), "info", 2});
    logger->flush();

    auto f = files();
    EXPECT_EQ(f, (std::vector<std::string>{"mds_2022-12-31.log", today_file(),
                                           "mds_notadate.log", "unrelated.txt"}));
}

TEST_F(LoggingTest, ZeroMaxFilesKeepsEverything) {
    for (int d = 1; d <= 5; ++d) touch("mds_2020-01-0" + std::to_string(d) + ".log");
    make_logger({(dir_ / "mds.log").string(), "info", 0});
    EXPECT_EQ(files().size(), 6u);  // five old + today's
}

// The level name is coloured in the file too, but only the level word, so
// `grep error` on the file still matches.
TEST_F(LoggingTest, FileLinesCarryAColouredLevelAndTheMessage) {
    auto logger = make_logger({(dir_ / "mds.log").string(), "info", 0});
    logger->warn("venue went quiet");
    logger->flush();

    auto text = read(today_file());
    EXPECT_NE(text.find("[mds] [\033[33;1mwarning\033[m] venue went quiet"), std::string::npos)
        << text;
}

TEST_F(LoggingTest, LevelFiltersLowerSeverity) {
    auto logger = make_logger({(dir_ / "mds.log").string(), "warn", 0});
    logger->info("should not appear");
    logger->error("should appear");
    logger->flush();

    auto text = read(today_file());
    EXPECT_EQ(text.find("should not appear"), std::string::npos);
    EXPECT_NE(text.find("should appear"), std::string::npos);
}

TEST_F(LoggingTest, EmptyFileMeansConsoleOnly) {
    auto logger = make_logger({"", "info", 2});
    logger->info("console only");
    EXPECT_TRUE(files().empty());
    EXPECT_EQ(logger->sinks().size(), 1u);
}

}  // namespace
}  // namespace mds_test
