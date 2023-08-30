#include <rclcpp/qos.hpp>
#include <rmw/qos_profiles.h>
#include "web_video_server/ros_compressed_streamer.h"

namespace web_video_server
{

RosCompressedStreamer::RosCompressedStreamer(const async_web_server_cpp::HttpRequest &request,
                             async_web_server_cpp::HttpConnectionPtr connection, rclcpp::Node::SharedPtr nh) :
  ImageStreamer(request, connection, nh), stream_(std::bind(&rclcpp::Node::now, nh), connection)
{
  stream_.sendInitialHeader();
}

RosCompressedStreamer::~RosCompressedStreamer()
{
  this->inactive_ = true;
  boost::mutex::scoped_lock lock(send_mutex_); // protects sendImage.
}

void RosCompressedStreamer::start() {

  // Create a QoS profile using the sensor message preset
  rmw_qos_profile_t sensor_qos = rmw_qos_profile_sensor_data;

  auto compressed_qos = rclcpp::QoS(rclcpp::QoSInitialization::from_rmw(sensor_qos));
  compressed_qos.get_rmw_qos_profile() = sensor_qos;

  std::string compressed_topic = topic_ + "/compressed";

  image_sub_ = nh_->create_subscription<sensor_msgs::msg::CompressedImage>(compressed_topic, compressed_qos, 
                                                                           std::bind(&RosCompressedStreamer::imageCallback, this, std::placeholders::_1));
}

void RosCompressedStreamer::restreamFrame(double max_age)
{
  if (inactive_ || (last_msg == 0))
    return;

  if ( last_frame + rclcpp::Duration::from_seconds(max_age) < nh_->now() ) {
    boost::mutex::scoped_lock lock(send_mutex_);
    sendImage(last_msg, nh_->now() ); // don't update last_frame, it may remain an old value.
  }
}

void RosCompressedStreamer::sendImage(const sensor_msgs::msg::CompressedImage::ConstSharedPtr msg,
                                      const rclcpp::Time &time) {
  try {
    std::string content_type;
    if(msg->format.find("jpeg") != std::string::npos) {
      content_type = "image/jpeg";
    }
    else if(msg->format.find("png") != std::string::npos) {
      content_type = "image/png";
    }
    else {
      RCLCPP_WARN(nh_->get_logger(), "Unknown ROS compressed image format: %s", msg->format.c_str());
      return;
    }

    stream_.sendPart(time, content_type, boost::asio::buffer(msg->data), msg);
  }
  catch (boost::system::system_error &e)
  {
    // happens when client disconnects
    RCLCPP_DEBUG(nh_->get_logger(), "system_error exception: %s", e.what());
    inactive_ = true;
    return;
  }
  catch (std::exception &e)
  {
    // TODO THROTTLE with 30
    RCLCPP_ERROR(nh_->get_logger(), "exception: %s", e.what());
    inactive_ = true;
    return;
  }
  catch (...)
  {
    // TODO THROTTLE with 30
    RCLCPP_ERROR(nh_->get_logger(), "exception");
    inactive_ = true;
    return;
  }
}


void RosCompressedStreamer::imageCallback(const sensor_msgs::msg::CompressedImage::ConstSharedPtr msg) {
  
  if (stream_.isBufferEmpty()){
    last_msg = msg;
    last_frame = rclcpp::Time(msg->header.stamp);
    boost::mutex::scoped_lock lock(send_mutex_); // protects last_msg and last_frame
    sendImage(last_msg, last_frame);
  }
}


boost::shared_ptr<ImageStreamer> RosCompressedStreamerType::create_streamer(const async_web_server_cpp::HttpRequest &request,
										 async_web_server_cpp::HttpConnectionPtr connection,
										 rclcpp::Node::SharedPtr nh)
{
  return boost::shared_ptr<ImageStreamer>(new RosCompressedStreamer(request, connection, nh));
}

std::string RosCompressedStreamerType::create_viewer(const async_web_server_cpp::HttpRequest &request)
{
  std::stringstream ss;
  ss << "<img src=\"/stream?";
  ss << request.query;
  ss << "\"></img>";
  return ss.str();
}

RosCompressedSnapshotStreamer::RosCompressedSnapshotStreamer(const async_web_server_cpp::HttpRequest &request,
                             async_web_server_cpp::HttpConnectionPtr connection, rclcpp::Node::SharedPtr nh) :
  ImageStreamer(request, connection, nh), stream_(std::bind(&rclcpp::Node::now, nh), connection)
{
}

RosCompressedSnapshotStreamer::~RosCompressedSnapshotStreamer()
{
  this->inactive_ = true;
  boost::mutex::scoped_lock lock(send_mutex_); // protects sendImage.
}

void RosCompressedSnapshotStreamer::start() {

  RCLCPP_INFO(nh_->get_logger(), "RosCompressedSnapshotStreamer::start()");
  // Create a QoS profile using the sensor message preset
  rmw_qos_profile_t sensor_qos = rmw_qos_profile_sensor_data;

  auto compressed_qos = rclcpp::QoS(rclcpp::QoSInitialization::from_rmw(sensor_qos));
  compressed_qos.get_rmw_qos_profile() = sensor_qos;

  std::string compressed_topic = topic_ + "/compressed";

  image_sub_ = nh_->create_subscription<sensor_msgs::msg::CompressedImage>(compressed_topic, compressed_qos, 
                                                                           std::bind(&RosCompressedSnapshotStreamer::imageCallback, this, std::placeholders::_1));
}

void RosCompressedSnapshotStreamer::restreamFrame(double max_age)
{
  if (inactive_ || (last_msg == 0))
    return;

  if ( last_frame + rclcpp::Duration::from_seconds(max_age) < nh_->now() ) {
    boost::mutex::scoped_lock lock(send_mutex_);
    sendImage(last_msg, nh_->now() ); // don't update last_frame, it may remain an old value.
  }
}

void RosCompressedSnapshotStreamer::sendImage(const sensor_msgs::msg::CompressedImage::ConstSharedPtr msg,
                                           const rclcpp::Time &time) {
  try {
    std::string content_type;
    if(msg->format.find("jpeg") != std::string::npos) {
      content_type = "image/jpeg";
    }
    else if(msg->format.find("png") != std::string::npos) {
      content_type = "image/png";
    }
    else {
      RCLCPP_WARN(nh_->get_logger(), "Unknown ROS compressed image format: %s", msg->format.c_str());
      return;
    }

    char stamp[20];
    sprintf(stamp, "%.06lf", time.seconds());
    async_web_server_cpp::HttpReply::builder(async_web_server_cpp::HttpReply::ok)
        .header("Connection", "close")
        .header("Server", "web_video_server")
        .header("Cache-Control",
                "no-cache, no-store, must-revalidate, pre-check=0, post-check=0, "
                "max-age=0")
        .header("X-Timestamp", stamp)
        .header("Pragma", "no-cache")
        .header("Content-type", "image/jpeg")
        .header("Access-Control-Allow-Origin", "*")
        .header("Content-Length",
                boost::lexical_cast<std::string>(msg->data.size()))
        .write(connection_);

    // TODO: this is a hack to get around the fact that write() doesn't take a const buffer
    // I know you shouldn't const_cast, but this is the only way to get it to compile
    // without changing the async_web_server code or copying the buffer. Maybe change async_web_server?
    connection_->write_and_clear(const_cast<std::vector<uchar>&>(msg->data));
    inactive_ = true;
  }

  catch (boost::system::system_error &e)
  {
    // happens when client disconnects
    RCLCPP_DEBUG(nh_->get_logger(), "system_error exception: %s", e.what());
    inactive_ = true;
    return;
  }
  catch (std::exception &e)
  {
    RCLCPP_ERROR(nh_->get_logger(), "exception: %s", e.what());
    inactive_ = true;
    return;
  }
  catch (...)
  {
    RCLCPP_ERROR(nh_->get_logger(), "exception");
    inactive_ = true;
    return;
  }
}

void RosCompressedSnapshotStreamer::imageCallback(const sensor_msgs::msg::CompressedImage::ConstSharedPtr msg) {
  
  if (stream_.isBufferEmpty()) {
    last_msg = msg;
    last_frame = rclcpp::Time(msg->header.stamp);
    boost::mutex::scoped_lock lock(send_mutex_); // protects last_msg and last_frame
    sendImage(last_msg, last_frame);

    // Remove the subscription to the topic
    image_sub_.reset();
  }
}

}
